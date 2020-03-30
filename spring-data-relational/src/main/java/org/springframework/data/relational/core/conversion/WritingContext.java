/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
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
 */
class WritingContext {

	private final RelationalMappingContext context;
	private final Object root;
	private final Object entity;
	private final Class<?> entityType;
	private final PersistentPropertyPaths<?, RelationalPersistentProperty> paths;
	private final Map<PathNode, DbAction<?>> previousActions = new HashMap<>();
	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, List<PathNode>> nodesCache = new HashMap<>();

	WritingContext(RelationalMappingContext context, Object root, MutableAggregateChange<?> aggregateChange) {

		this.context = context;
		this.root = root;
		this.entity = aggregateChange.getEntity();
		this.entityType = aggregateChange.getEntityType();
		this.paths = context.findPersistentPropertyPaths(entityType, (p) -> p.isEntity() && !p.isEmbedded());
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282
	 *
	 * @return List of {@link DbAction}s
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DAJDBC-282</a>
	 */
	List<DbAction<?>> insert() {

		List<DbAction<?>> actions = new ArrayList<>();
		actions.add(setRootAction(new DbAction.InsertRoot<>(entity)));
		actions.addAll(insertReferenced());
		return actions;
	}

	/**
	 * Leaves out the isNew check as defined in #DATAJDBC-282 Possible Deadlocks in Execution Order in #DATAJDBC-488
	 *
	 * @return List of {@link DbAction}s
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DAJDBC-282</a>
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-488">DAJDBC-488</a>
	 */
	List<DbAction<?>> update() {

		List<DbAction<?>> actions = new ArrayList<>();
		actions.add(setRootAction(new DbAction.UpdateRoot<>(entity)));
		actions.addAll(deleteReferenced());
		actions.addAll(insertReferenced());
		return actions;
	}

	List<DbAction<?>> save() {

		List<DbAction<?>> actions = new ArrayList<>();
		if (isNew(root)) {

			actions.add(setRootAction(new DbAction.InsertRoot<>(entity)));
			actions.addAll(insertReferenced());
		} else {

			actions.add(setRootAction(new DbAction.UpdateRoot<>(entity)));
			actions.addAll(deleteReferenced());
			actions.addAll(insertReferenced());
		}

		return actions;
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
	private List<DbAction<?>> insertAll(PersistentPropertyPath<RelationalPersistentProperty> path) {

		List<DbAction<?>> actions = new ArrayList<>();

		from(path).forEach(node -> {

			DbAction.WithEntity<?> parentAction = getAction(node.getParent());
			DbAction.Insert<Object> insert;
			if (node.getPath().getRequiredLeafProperty().isQualified()) {

				Pair<Object, Object> value = (Pair) node.getValue();
				Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers = new HashMap<>();
				qualifiers.put(node.getPath(), value.getFirst());

				RelationalPersistentEntity<?> parentEntity = context.getRequiredPersistentEntity(parentAction.getEntityType());

				if (!parentEntity.hasIdProperty() && parentAction instanceof DbAction.Insert) {
					qualifiers.putAll(((DbAction.Insert<?>) parentAction).getQualifiers());
				}
				insert = new DbAction.Insert<>(value.getSecond(), path, parentAction, qualifiers);

			} else {
				insert = new DbAction.Insert<>(node.getValue(), path, parentAction, new HashMap<>());
			}
			previousActions.put(node, insert);
			actions.add(insert);
		});

		return actions;
	}

	private List<DbAction<?>> deleteReferenced() {

		List<DbAction<?>> deletes = new ArrayList<>();
		paths.forEach(path -> deletes.add(0, deleteReferenced(path)));

		return deletes;
	}

	/// Operations on a single path

	private DbAction.Delete<?> deleteReferenced(PersistentPropertyPath<RelationalPersistentProperty> path) {

		Object id = context.getRequiredPersistentEntity(entityType).getIdentifierAccessor(entity).getIdentifier();

		return new DbAction.Delete<>(id, path);
	}

	//// methods not directly related to the creation of DbActions

	private DbAction<?> setRootAction(DbAction<?> dbAction) {

		previousActions.put(null, dbAction);
		return dbAction;
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

				Object value = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(parentValue)
						.getProperty(path.getRequiredLeafProperty());

				nodes.addAll(createNodes(path, parentNode, value));
			});
		}

		nodesCache.put(path, nodes);

		return nodes;
	}

	private boolean isDirectlyReferencedByRootIgnoringEmbeddables(
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		PersistentPropertyPath<RelationalPersistentProperty> currentPath = path.getParentPath();

		while (!currentPath.isEmpty()) {

			if (!currentPath.getRequiredLeafProperty().isEmbedded()) {
				return false;
			}
			currentPath = currentPath.getParentPath();
		}

		return true;
	}

	@Nullable
	private Object getFromRootValue(PersistentPropertyPath<RelationalPersistentProperty> path) {

		if (path.getLength() == 0) {
			return entity;
		}

		Object parent = getFromRootValue(path.getParentPath());
		if (parent == null) {
			return null;
		}

		return context.getRequiredPersistentEntity(parent.getClass()).getPropertyAccessor(parent)
				.getProperty(path.getRequiredLeafProperty());
	}

	private List<PathNode> createNodes(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable PathNode parentNode, @Nullable Object value) {

		if (value == null) {
			return Collections.emptyList();
		}

		List<PathNode> nodes = new ArrayList<>();
		if (path.getRequiredLeafProperty().isEmbedded()) {
			nodes.add(new PathNode(path, parentNode, value));
		} else if (path.getRequiredLeafProperty().isQualified()) {

			if (path.getRequiredLeafProperty().isMap()) {
				((Map<?, ?>) value).forEach((k, v) -> nodes.add(new PathNode(path, parentNode, Pair.of(k, v))));
			} else {

				List<Object> listValue = (List<Object>) value;
				for (int k = 0; k < listValue.size(); k++) {
					nodes.add(new PathNode(path, parentNode, Pair.of(k, listValue.get(k))));
				}
			}
		} else if (path.getRequiredLeafProperty().isCollectionLike()) { // collection value
			if (value.getClass().isArray()) {
				Arrays.asList((Object[]) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
			} else {
				((Iterable<?>) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
			}
		} else { // single entity value
			nodes.add(new PathNode(path, parentNode, value));
		}

		return nodes;
	}

}
