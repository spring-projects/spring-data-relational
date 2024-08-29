/*
 * Copyright 2019-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.conversion;

import static java.util.Arrays.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Holds context information for the current save operation.
 *
 * @author Jens Schauder
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
class WritingContext<T> {

	private final RelationalMappingContext context;
	private final T root;
	private final Class<T> entityType;
	private final List<PersistentPropertyPath<RelationalPersistentProperty>> paths;
	private final Map<PathNode, DbAction<?>> previousActions = new HashMap<>();
	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, List<PathNode>> nodesCache = new HashMap<>();
	private final IdValueSource rootIdValueSource;
	@Nullable private final Number previousVersion;
	private final RootAggregateChange<T> aggregateChange;

	WritingContext(RelationalMappingContext context, T root, RootAggregateChange<T> aggregateChange) {

		this.context = context;
		this.root = root;
		this.entityType = aggregateChange.getEntityType();
		this.previousVersion = aggregateChange.getPreviousVersion();
		this.aggregateChange = aggregateChange;
		this.rootIdValueSource = IdValueSource.forInstance(root,
				context.getRequiredPersistentEntity(aggregateChange.getEntityType()));
		this.paths = context.findPersistentPropertyPaths(entityType, (p) -> p.isEntity() && !p.isEmbedded()) //
				.filter(ppp -> context.getAggregatePath(ppp).isWritable()).toList();
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282
	 *
	 * @see <a href="https://github.com/spring-projects/spring-data-jdbc/issues/507">DAJDBC-282</a>
	 */
	void insert() {

		setRootAction(new DbAction.InsertRoot<>(root, rootIdValueSource));
		insertReferenced().forEach(aggregateChange::addAction);
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282 Possible Deadlocks in Execution Order in #DATAJDBC-488
	 *
	 * @see <a href="https://github.com/spring-projects/spring-data-jdbc/issues/507">DAJDBC-282</a>
	 * @see <a href="https://github.com/spring-projects/spring-data-jdbc/issues/714">DAJDBC-488</a>
	 */
	void update() {

		setRootAction(new DbAction.UpdateRoot<>(root, previousVersion));
		deleteReferenced().forEach(aggregateChange::addAction);
		insertReferenced().forEach(aggregateChange::addAction);
	}

	void save() {

		if (isNew(root)) {

			setRootAction(new DbAction.InsertRoot<>(root, rootIdValueSource));
			insertReferenced().forEach(aggregateChange::addAction);
		} else {

			setRootAction(new DbAction.UpdateRoot<>(root, previousVersion));
			deleteReferenced().forEach(aggregateChange::addAction);
			insertReferenced().forEach(aggregateChange::addAction);
		}
	}

	private boolean isNew(Object o) {
		return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
	}

	//// Operations on all paths

	private List<DbAction<?>> insertReferenced() {

		List<DbAction<?>> actions = new ArrayList<>();

		paths.forEach(path -> actions.addAll(insertAll(path)));

		return actions;
	}

	@SuppressWarnings("unchecked")
	private List<? extends DbAction<?>> insertAll(PersistentPropertyPath<RelationalPersistentProperty> path) {

		RelationalPersistentEntity<?> persistentEntity = context
				.getRequiredPersistentEntity(path.getLeafProperty());
		List<DbAction.Insert<Object>> inserts = new ArrayList<>();
		from(path).forEach(node -> {

			DbAction.WithEntity<?> parentAction = getAction(node.getParent());
			Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers = new HashMap<>();
			Object instance;
			if (node.getPath().getLeafProperty().isQualified()) {

				Pair<Object, Object> value = (Pair) node.getValue();
				qualifiers.put(node.getPath(), value.getFirst());

				RelationalPersistentEntity<?> parentEntity = context.getRequiredPersistentEntity(parentAction.getEntityType());

				if (!parentEntity.hasIdProperty() && parentAction instanceof DbAction.Insert) {
					qualifiers.putAll(((DbAction.Insert<?>) parentAction).getQualifiers());
				}
				instance = value.getSecond();
			} else {
				instance = node.getValue();
			}
			IdValueSource idValueSource = IdValueSource.forInstance(instance, persistentEntity);
			DbAction.Insert<Object> insert = new DbAction.Insert<>(instance, path, parentAction, qualifiers, idValueSource);
			inserts.add(insert);
			previousActions.put(node, insert);
		});
		return inserts;
	}

	private List<DbAction<?>> deleteReferenced() {

		List<DbAction<?>> deletes = new ArrayList<>();
		paths.forEach(path -> deletes.add(0, deleteReferenced(path)));

		return deletes;
	}

	/// Operations on a single path

	private DbAction.Delete<?> deleteReferenced(PersistentPropertyPath<RelationalPersistentProperty> path) {

		Object id = context.getRequiredPersistentEntity(entityType).getIdentifierAccessor(root).getIdentifier();

		return new DbAction.Delete<>(id, path);
	}

	//// methods not directly related to the creation of DbActions

	private void setRootAction(DbAction.WithRoot<T> dbAction) {
		aggregateChange.setRootAction(dbAction);
		previousActions.put(null, dbAction);
	}

	@Nullable
	private DbAction.WithEntity<?> getAction(@Nullable PathNode parent) {

		DbAction<?> action = previousActions.get(parent);

		if (action != null) {

			Assert.isInstanceOf( //
					DbAction.WithEntity.class, //
					action, //
					"dependsOn action is not a WithEntity, but " + action.getClass().getSimpleName() //
			);

			return (DbAction.WithEntity<?>) action;
		}

		return null;
	}

	private List<PathNode> from(PersistentPropertyPath<RelationalPersistentProperty> path) {

		List<PathNode> nodes = new ArrayList<>();

		if (isDirectlyReferencedByRootIgnoringEmbeddables(path)) {

			Object value = getFromRootValue(path);
			nodes.addAll(createNodes(path, null, value));

		} else {

			List<PathNode> pathNodes = nodesCache.getOrDefault(path.getParentPath(), Collections.emptyList());

			pathNodes.forEach(parentNode -> {

				// todo: this should go into pathnode
				Object parentValue = parentNode.getActualValue();

				Object value = path.getLeafProperty().getOwner().getPropertyAccessor(parentValue)
						.getProperty(path.getLeafProperty());

				nodes.addAll(createNodes(path, parentNode, value));
			});
		}

		nodesCache.put(path, nodes);

		return nodes;
	}

	private boolean isDirectlyReferencedByRootIgnoringEmbeddables(
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		PersistentPropertyPath<RelationalPersistentProperty> currentPath = path.getParentPath();

		while (currentPath != null) {

			if (!currentPath.getLeafProperty().isEmbedded()) {
				return false;
			}
			currentPath = currentPath.getParentPath();
		}

		return true;
	}

	@Nullable
	private Object getFromRootValue(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		if (path == null) {
			return root;
		}

		Object parent = getFromRootValue(path.getParentPath());
		if (parent == null) {
			return null;
		}

		return context.getRequiredPersistentEntity(parent.getClass()).getPropertyAccessor(parent)
				.getProperty(path.getLeafProperty());
	}

	private List<PathNode> createNodes(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable PathNode parentNode, @Nullable Object value) {

		if (value == null) {
			return Collections.emptyList();
		}

		List<PathNode> nodes = new ArrayList<>();
		if (path.getLeafProperty().isEmbedded()) {
			nodes.add(new PathNode(path, parentNode, value));
		} else if (path.getLeafProperty().isQualified()) {

			if (path.getLeafProperty().isMap()) {
				((Map<?, ?>) value).forEach((k, v) -> nodes.add(new PathNode(path, parentNode, Pair.of(k, v))));
			} else {

				List<Object> listValue = (List<Object>) value;
				for (int k = 0; k < listValue.size(); k++) {
					nodes.add(new PathNode(path, parentNode, Pair.of(k, listValue.get(k))));
				}
			}
		} else if (path.getLeafProperty().isCollectionLike()) { // collection value
			if (value.getClass().isArray()) {
				asList((Object[]) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
			} else {
				((Iterable<?>) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
			}
		} else { // single entity value
			nodes.add(new PathNode(path, parentNode, value));
		}

		return nodes;
	}

}
