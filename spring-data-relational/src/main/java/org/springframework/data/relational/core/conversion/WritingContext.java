/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.conversion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
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
 */
class WritingContext {

	private final RelationalMappingContext context;
	private final Object root;
	private final Object entity;
	private final Class<?> entityType;
	private final PersistentPropertyPaths<?, RelationalPersistentProperty> paths;
	private final Map<PathNode, DbAction> previousActions = new HashMap<>();
	private Map<PersistentPropertyPath<RelationalPersistentProperty>, List<PathNode>> nodesCache = new HashMap<>();

	WritingContext(RelationalMappingContext context, Object root, AggregateChange<?> aggregateChange) {

		this.context = context;
		this.root = root;
		this.entity = aggregateChange.getEntity();
		this.entityType = aggregateChange.getEntityType();
		this.paths = context.findPersistentPropertyPaths(entityType, PersistentProperty::isEntity);
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
	 * Leaves out the isNew check as defined in #DATAJDBC-282
	 *
	 * @return List of {@link DbAction}s
	 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DAJDBC-282</a>
	 */
	List<DbAction<?>> update() {

		List<DbAction<?>> actions = new ArrayList<>(deleteReferenced());
		actions.add(setRootAction(new DbAction.UpdateRoot<>(entity)));
		actions.addAll(insertReferenced());
		return actions;
	}

	List<DbAction<?>> save() {

		List<DbAction<?>> actions = new ArrayList<>();
		if (isNew(root)) {

			actions.add(setRootAction(new DbAction.InsertRoot<>(entity)));
			actions.addAll(insertReferenced());
		} else {

			actions.addAll(deleteReferenced());
			actions.add(setRootAction(new DbAction.UpdateRoot<>(entity)));
			actions.addAll(insertReferenced());
		}

		return actions;
	}

	//// Operations on all paths

	private List<DbAction<?>> insertReferenced() {

		List<DbAction<?>> actions = new ArrayList<>();

		paths.forEach(path -> actions.addAll(insertAll(path)));

		return actions;
	}

	private List<DbAction<?>> insertAll(PersistentPropertyPath<RelationalPersistentProperty> path) {

		List<DbAction<?>> actions = new ArrayList<>();

		from(path).forEach(node -> {

			DbAction.Insert<Object> insert;
			if (node.getPath().getRequiredLeafProperty().isQualified()) {

				Pair<Object, Object> value = (Pair) node.getValue();
				insert = new DbAction.Insert<>(value.getSecond(), path, getAction(node.getParent()));
				insert.getAdditionalValues().put(node.getPath().getRequiredLeafProperty().getKeyColumn(), value.getFirst());

				insert.addKey(node.getPath(), value.getFirst());
			} else {
				insert = new DbAction.Insert<>(node.getValue(), path, getAction(node.getParent()));
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

		DbAction action = previousActions.get(parent);

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

	private boolean isNew(Object o) {
		return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
	}

	private List<PathNode> from(PersistentPropertyPath<RelationalPersistentProperty> path) {

		List<PathNode> nodes = new ArrayList<>();

		RelationalPersistentProperty property = path.getRequiredLeafProperty();
		if (path.getLength() == 1) {

			Object value = context //
					.getRequiredPersistentEntity(entityType) //
					.getPropertyAccessor(entity) //
					.getProperty(property);

			nodes.addAll(createNodes(path, null, value));

		} else {

			List<PathNode> pathNodes = nodesCache.get(path.getParentPath());
			pathNodes.forEach(parentPathNode -> {

				RelationalPersistentEntity<?> parentType = property.getOwner();
				Object parentNodeValue = parentPathNode.getValue();

				RelationalPersistentProperty parentProperty = path.getParentPath().getLeafProperty();
				boolean parentIsQualified = parentProperty != null && parentProperty.isQualified();

				if (parentIsQualified) {

					Assert.isInstanceOf(Pair.class, parentNodeValue,
							"The parentValue is expected to be of type Pair. This is a bug please file an issue at https://jira.spring.io/projects/DATAJDBC !");

					Object value = parentType.getPropertyAccessor(((Pair) parentNodeValue).getSecond()).getProperty(property);
					nodes.addAll(createNodes(path, parentPathNode, value));

				} else {

					PersistentPropertyAccessor<Object> parentValueAccessor = parentType.getPropertyAccessor(parentNodeValue);
					Object value = parentValueAccessor.getProperty(property);

					nodes.addAll(createNodes(path, parentPathNode, value));
				}
			});
		}

		nodesCache.put(path, nodes);

		return nodes;
	}

	private List<PathNode> createNodes(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable PathNode parentNode, @Nullable Object value) {

		if (value == null) {
			return Collections.emptyList();
		}

		List<PathNode> nodes = new ArrayList<>();

		if (path.getRequiredLeafProperty().isQualified()) {

			if (path.getRequiredLeafProperty().isMap()) {

				((Map<?, ?>) value).forEach( //
						(k, v) -> nodes.add(new PathNode(path, parentNode, Pair.of(k, v))) //
				);
			} else {

				List listValue = (List) value;
				for (int k = 0; k < listValue.size(); k++) {
					nodes.add(new PathNode(path, parentNode, Pair.of(k, listValue.get(k))));
				}
			}
		} else if (path.getRequiredLeafProperty().isCollectionLike()) { // collection value
			((Collection<?>) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
		} else { // single entity value
			nodes.add(new PathNode(path, parentNode, value));
		}

		return nodes;
	}

}
