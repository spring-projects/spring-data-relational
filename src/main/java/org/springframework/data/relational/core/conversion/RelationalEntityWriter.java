/*
 * Copyright 2017-2018 the original author or authors.
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

import lombok.NonNull;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Converts an aggregate represented by its root into an {@link AggregateChange}.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class RelationalEntityWriter implements EntityWriter<Object, AggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityWriter(RelationalMappingContext context) {
		this.context = context;
	}

	@Override
	public void write(Object root, AggregateChange<?> aggregateChange) {

		new WritingContext(root, aggregateChange).write();
	}

	/**
	 * Holds context information for the current write operation.
	 */
	private class WritingContext {

		private final Object root;
		private final AggregateChange<?> aggregateChange;
		private final PersistentPropertyPaths<?, RelationalPersistentProperty> paths;
		private final Map<PathNode, DbAction> previousActions = new HashMap<>();
		private Map<PersistentPropertyPath<RelationalPersistentProperty>, List<PathNode>> nodesCache = new HashMap<>();

		WritingContext(Object root, AggregateChange<?> aggregateChange) {

			this.root = root;
			this.aggregateChange = aggregateChange;

			paths = context.findPersistentPropertyPaths(aggregateChange.getEntityType(), PersistentProperty::isEntity);

		}

		private void write() {

			if (isNew(root)) {

				setRootAction(new DbAction.InsertRoot<>(aggregateChange.getEntity()));
				insertReferenced();
			} else {
				deleteReferenced();

				setRootAction(new DbAction.UpdateRoot<>(aggregateChange.getEntity()));
				insertReferenced();
			}
		}

		//// Operations on all paths

		private void insertReferenced() {

			paths.forEach(this::insertAll);

		}

		private void insertAll(PersistentPropertyPath<RelationalPersistentProperty> path) {

			from(path).forEach(node -> {

				DbAction.Insert<Object> insert;
				if (node.path.getRequiredLeafProperty().isQualified()) {

					KeyValue value = (KeyValue) node.getValue();
					insert = new DbAction.Insert<>(value.value, path, getAction(node.parent));
					insert.getAdditionalValues().put(node.path.getRequiredLeafProperty().getKeyColumn(), value.key);

				} else {
					insert = new DbAction.Insert<>(node.getValue(), path, getAction(node.parent));
				}

				previousActions.put(node, insert);
				aggregateChange.addAction(insert);
			});
		}

		private void deleteReferenced() {

			ArrayList<DbAction> deletes = new ArrayList<>();
			paths.forEach(path -> deletes.add(0, deleteReferenced(path)));

			deletes.forEach(aggregateChange::addAction);
		}

		/// Operations on a single path

		private DbAction.Delete<?> deleteReferenced(PersistentPropertyPath<RelationalPersistentProperty> path) {

			Object id = context.getRequiredPersistentEntity(aggregateChange.getEntityType())
					.getIdentifierAccessor(aggregateChange.getEntity()).getIdentifier();

			return new DbAction.Delete<>(id, path);
		}

		//// methods not directly related to the creation of DbActions

		private void setRootAction(DbAction<?> insert) {

			previousActions.put(null, insert);
			aggregateChange.addAction(insert);
		}

		@Nullable
		private DbAction.WithEntity<?> getAction(@Nullable PathNode parent) {

			DbAction action = previousActions.get(parent);
			if (action != null) {
				Assert.isInstanceOf(DbAction.WithEntity.class, action,
						"dependsOn action is not a WithEntity, but " + action.getClass().getSimpleName());
				return (DbAction.WithEntity<?>) action;
			}
			return null;
		}

		private boolean isNew(Object o) {
			return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
		}

		private List<WritingContext.PathNode> from(PersistentPropertyPath<RelationalPersistentProperty> path) {

			List<WritingContext.PathNode> nodes = new ArrayList<>();
			if (path.getLength() == 1) {

				Object value = context //
						.getRequiredPersistentEntity(aggregateChange.getEntityType()) //
						.getPropertyAccessor(aggregateChange.getEntity()) //
						.getProperty(path.getRequiredLeafProperty());

				nodes.addAll(createNodes(path, null, value));

			} else {

				List<PathNode> pathNodes = nodesCache.get(path.getParentPath());
				pathNodes.forEach(parentNode -> {

					Object value = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(parentNode.value)
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

			List<WritingContext.PathNode> nodes = new ArrayList<>();

			if (path.getRequiredLeafProperty().isQualified()) {

				if (path.getRequiredLeafProperty().isMap()) {
					((Map<?, ?>) value).forEach((k, v) -> nodes.add(new PathNode(path, parentNode, new KeyValue(k, v))));
				} else {

					List listValue = (List) value;
					for (int k = 0; k < listValue.size(); k++) {
						nodes.add(new PathNode(path, parentNode, new KeyValue(k, listValue.get(k))));
					}
				}
			} else if (path.getRequiredLeafProperty().isCollectionLike()) { // collection value
				((Collection<?>) value).forEach(v -> nodes.add(new PathNode(path, parentNode, v)));
			} else { // single entity value
				nodes.add(new PathNode(path, parentNode, value));
			}

			return nodes;
		}

		/**
		 * represents a single entity in an aggregate along with its property path from the root entity and the chain of
		 * objects to traverse a long this path.
		 */
		private class PathNode {

			private final PersistentPropertyPath<RelationalPersistentProperty> path;
			@Nullable private final PathNode parent;
			private final Object value;

			private PathNode(PersistentPropertyPath<RelationalPersistentProperty> path, @Nullable PathNode parent,
					Object value) {

				this.path = path;
				this.parent = parent;
				this.value = value;
			}

			public Object getValue() {
				return value;
			}

		}
	}

	/**
	 * Holds key and value of a {@link Map.Entry} but without any ties to {@link Map} implementations.
	 */
	@Value
	private static class KeyValue {
		@NonNull Object key;
		@NonNull Object value;
	}

}
