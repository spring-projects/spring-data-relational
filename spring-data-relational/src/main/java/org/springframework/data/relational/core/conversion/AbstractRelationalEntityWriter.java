package org.springframework.data.relational.core.conversion;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used as an abstract class to derive relational writer actions (save, insert, update).
 * Implementations see {@link RelationalEntityWriter} and {@link RelationalEntityInsertWriter}
 *
 * @author Thomas Lang
 */
abstract class AbstractRelationalEntityWriter implements EntityWriter<Object, AggregateChange<?>> {

	protected final RelationalMappingContext context;

	AbstractRelationalEntityWriter(RelationalMappingContext context) {
		this.context = context;
	}

	/**
	 * Holds context information for the current save operation.
	 */
	class WritingContext {

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

		private boolean isNew(Object o) {
			return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
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

		@Nullable private DbAction.WithEntity<?> getAction(@Nullable RelationalEntityInsertWriter.PathNode parent) {

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
		//        commented as of #DATAJDBC-282
		//        private boolean isNew(Object o) {
		//            return context.getRequiredPersistentEntity(o.getClass()).isNew(o);
		//        }

		private List<RelationalEntityInsertWriter.PathNode> from(
				PersistentPropertyPath<RelationalPersistentProperty> path) {

			List<RelationalEntityInsertWriter.PathNode> nodes = new ArrayList<>();

			if (path.getLength() == 1) {

				Object value = context //
						.getRequiredPersistentEntity(entityType) //
						.getPropertyAccessor(entity) //
						.getProperty(path.getRequiredLeafProperty());

				nodes.addAll(createNodes(path, null, value));

			} else {

				List<RelationalEntityInsertWriter.PathNode> pathNodes = nodesCache.get(path.getParentPath());
				pathNodes.forEach(parentNode -> {

					Object value = path.getRequiredLeafProperty().getOwner().getPropertyAccessor(parentNode.getValue())
							.getProperty(path.getRequiredLeafProperty());

					nodes.addAll(createNodes(path, parentNode, value));
				});
			}

			nodesCache.put(path, nodes);

			return nodes;
		}

		private List<RelationalEntityInsertWriter.PathNode> createNodes(
				PersistentPropertyPath<RelationalPersistentProperty> path,
				@Nullable RelationalEntityInsertWriter.PathNode parentNode, @Nullable Object value) {

			if (value == null) {
				return Collections.emptyList();
			}

			List<RelationalEntityInsertWriter.PathNode> nodes = new ArrayList<>();

			if (path.getRequiredLeafProperty().isQualified()) {

				if (path.getRequiredLeafProperty().isMap()) {
					((Map<?, ?>) value)
							.forEach((k, v) -> nodes.add(new RelationalEntityInsertWriter.PathNode(path, parentNode, Pair.of(k, v))));
				} else {

					List listValue = (List) value;
					for (int k = 0; k < listValue.size(); k++) {
						nodes.add(new RelationalEntityInsertWriter.PathNode(path, parentNode, Pair.of(k, listValue.get(k))));
					}
				}
			} else if (path.getRequiredLeafProperty().isCollectionLike()) { // collection value
				((Collection<?>) value).forEach(v -> nodes.add(new RelationalEntityInsertWriter.PathNode(path, parentNode, v)));
			} else { // single entity value
				nodes.add(new RelationalEntityInsertWriter.PathNode(path, parentNode, value));
			}

			return nodes;
		}

	}

	/**
	 * Represents a single entity in an aggregate along with its property path from the root entity and the chain of
	 * objects to traverse a long this path.
	 */
	@Value static class PathNode {

		/**
		 * The path to this entity
		 */
		PersistentPropertyPath<RelationalPersistentProperty> path;

		/**
		 * The parent {@link PathNode}. This is {@code null} if this is the root entity.
		 */
		@Nullable PathNode parent;

		/**
		 * The value of the entity.
		 */
		Object value;
	}
}
