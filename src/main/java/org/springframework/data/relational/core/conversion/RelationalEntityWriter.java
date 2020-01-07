/*
 * Copyright 2017-2020 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Value;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Converts an aggregate represented by its root into an {@link AggregateChange}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class RelationalEntityWriter implements EntityWriter<Object, AggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityWriter(RelationalMappingContext context) {
		this.context = context;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object root, AggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new WritingContext(root, aggregateChange).write();

		actions.forEach(aggregateChange::addAction);
	}

	/**
	 * Holds context information for the current write operation.
	 */
	private class WritingContext {

		private final Object root;
		private final Object entity;
		private final Class<?> entityType;
		private final PersistentPropertyPaths<?, RelationalPersistentProperty> paths;
		private final Map<PathNode, DbAction> previousActions = new HashMap<>();
		private Map<PersistentPropertyPath<RelationalPersistentProperty>, List<PathNode>> nodesCache = new HashMap<>();

		WritingContext(Object root, AggregateChange<?> aggregateChange) {

			this.root = root;
			this.entity = aggregateChange.getEntity();
			this.entityType = aggregateChange.getEntityType();
			this.paths = context.findPersistentPropertyPaths(entityType, PersistentProperty::isEntity);
		}

		private List<DbAction<?>> write() {

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
				if (node.path.getRequiredLeafProperty().isQualified()) {

					Pair<Object, Object> value = (Pair) node.getValue();
					insert = new DbAction.Insert<>(value.getSecond(), path, getAction(node.parent));
					insert.getAdditionalValues().put(node.path.getRequiredLeafProperty().getKeyColumn(), value.getFirst());

				} else {
					insert = new DbAction.Insert<>(node.getValue(), path, getAction(node.parent));
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

			if (path.getLength() == 1) {

				Object value = context //
						.getRequiredPersistentEntity(entityType) //
						.getPropertyAccessor(entity) //
						.getProperty(path.getRequiredLeafProperty());

				nodes.addAll(createNodes(path, null, value));

			} else {

				List<PathNode> pathNodes = nodesCache.get(path.getParentPath());
				pathNodes.forEach(parentNode -> {

					Object value = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(parentNode.getValue())
							.getProperty(path.getRequiredLeafProperty());

					nodes.addAll(createNodes(path, parentNode, value));
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
					((Map<?, ?>) value).forEach((k, v) -> nodes.add(new PathNode(path, parentNode, Pair.of(k, v))));
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

	/**
	 * Represents a single entity in an aggregate along with its property path from the root entity and the chain of objects
	 * to traverse a long this path.
	 */
	@Value
	static class PathNode {

		/** The path to this entity */
		PersistentPropertyPath<RelationalPersistentProperty> path;

		/**
		 * The parent {@link PathNode}. This is {@code null} if this is
		 * the root entity.
		 */
		@Nullable
		PathNode parent;

		/** The value of the entity. */
		Object value;
	}
}
