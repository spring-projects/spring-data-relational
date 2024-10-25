/*
 * Copyright 2018-2024 the original author or authors.
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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An instance of this interface represents a (conceptual) single interaction with a database, e.g. a single update,
 * used as a step when synchronizing the state of an aggregate with the database.
 *
 * @param <T> the type of the entity that is affected by this action.
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
public interface DbAction<T> {

	Class<T> getEntityType();

	/**
	 * Represents an insert statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class Insert<T> implements WithDependingOn<T> {

		private final T entity;
		private final PersistentPropertyPath<RelationalPersistentProperty> propertyPath;
		private final WithEntity<?> dependingOn;
		private final IdValueSource idValueSource;

		final Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers;

		public Insert(T entity, PersistentPropertyPath<RelationalPersistentProperty> propertyPath,
				WithEntity<?> dependingOn, Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers,
				IdValueSource idValueSource) {

			this.entity = entity;
			this.propertyPath = propertyPath;
			this.dependingOn = dependingOn;
			this.qualifiers = Map.copyOf(qualifiers);
			this.idValueSource = idValueSource;
		}

		@Override
		public Class<T> getEntityType() {
			return WithDependingOn.super.getEntityType();
		}

		public T getEntity() {
			return this.entity;
		}

		public PersistentPropertyPath<RelationalPersistentProperty> getPropertyPath() {
			return this.propertyPath;
		}

		public DbAction.WithEntity<?> getDependingOn() {
			return this.dependingOn;
		}

		public Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> getQualifiers() {
			return this.qualifiers;
		}

		public IdValueSource getIdValueSource() {
			return idValueSource;
		}

		@Override
		public String toString() {
			return "Insert{" + "entity=" + entity + ", propertyPath=" + propertyPath + ", dependingOn=" + dependingOn
					+ ", idValueSource=" + idValueSource + ", qualifiers=" + qualifiers + '}';
		}
	}

	/**
	 * Represents an insert statement for the root of an aggregate. Upon a successful insert, the initial version and
	 * generated ids are populated.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class InsertRoot<T> implements WithRoot<T> {

		private T entity;
		private final IdValueSource idValueSource;

		public InsertRoot(T entity, IdValueSource idValueSource) {

			this.entity = entity;
			this.idValueSource = idValueSource;
		}

		public T getEntity() {
			return this.entity;
		}

		@Override
		public void setEntity(T entity) {
			this.entity = entity;
		}

		public IdValueSource getIdValueSource() {
			return idValueSource;
		}

		@Override
		public String toString() {
			return "InsertRoot{" + "entity=" + entity + ", idValueSource=" + idValueSource + '}';
		}
	}

	/**
	 * Represents an update statement for the aggregate root.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class UpdateRoot<T> implements WithRoot<T> {

		private T entity;

		@Nullable private final Number previousVersion;

		public UpdateRoot(T entity, @Nullable Number previousVersion) {
			this.entity = entity;
			this.previousVersion = previousVersion;
		}

		public T getEntity() {
			return this.entity;
		}

		@Override
		public void setEntity(T entity) {
			this.entity = entity;
		}

		@Override
		public IdValueSource getIdValueSource() {
			return IdValueSource.PROVIDED;
		}

		@Nullable
		public Number getPreviousVersion() {
			return previousVersion;
		}

		public String toString() {
			return "DbAction.UpdateRoot(entity=" + this.getEntity() + ")";
		}
	}

	/**
	 * Represents a delete statement for all entities that that a reachable via a give path from the aggregate root.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class Delete<T> implements WithPropertyPath<T> {

		private final Object rootId;

		private final PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		public Delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
			this.rootId = rootId;
			this.propertyPath = propertyPath;
		}

		public Object getRootId() {
			return this.rootId;
		}

		public PersistentPropertyPath<RelationalPersistentProperty> getPropertyPath() {
			return this.propertyPath;
		}

		public String toString() {
			return "DbAction.Delete(rootId=" + this.getRootId() + ", propertyPath=" + this.getPropertyPath() + ")";
		}
	}

	/**
	 * Represents a delete statement for a aggregate root when only the ID is known.
	 * <p>
	 * Note that deletes for contained entities that reference the root are to be represented by separate
	 * {@link DbAction}s.
	 * </p>
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class DeleteRoot<T> implements DbAction<T> {

		private final Object id;
		private final Class<T> entityType;
		@Nullable private final Number previousVersion;

		public DeleteRoot(Object id, Class<T> entityType, @Nullable Number previousVersion) {

			this.id = id;
			this.entityType = entityType;
			this.previousVersion = previousVersion;
		}

		public Object getId() {
			return this.id;
		}

		public Class<T> getEntityType() {
			return this.entityType;
		}

		@Nullable
		public Number getPreviousVersion() {
			return this.previousVersion;
		}

		public String toString() {

			return "DbAction.DeleteRoot(id=" + this.getId() + ", entityType=" + this.getEntityType() + ", previousVersion="
					+ this.getPreviousVersion() + ")";
		}
	}

	/**
	 * Represents an delete statement for all entities that that a reachable via a give path from any aggregate root of a
	 * given type.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class DeleteAll<T> implements WithPropertyPath<T> {

		private final PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		public DeleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
			this.propertyPath = propertyPath;
		}

		public PersistentPropertyPath<RelationalPersistentProperty> getPropertyPath() {
			return this.propertyPath;
		}

		public String toString() {
			return "DbAction.DeleteAll(propertyPath=" + this.getPropertyPath() + ")";
		}
	}

	/**
	 * Represents a delete statement for all aggregate roots of a given type.
	 * <p>
	 * Note that deletes for contained entities that reference the root are to be represented by separate
	 * {@link DbAction}s.
	 * </p>
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class DeleteAllRoot<T> implements DbAction<T> {

		private final Class<T> entityType;

		public DeleteAllRoot(Class<T> entityType) {
			this.entityType = entityType;
		}

		public Class<T> getEntityType() {
			return this.entityType;
		}

		public String toString() {
			return "DbAction.DeleteAllRoot(entityType=" + this.getEntityType() + ")";
		}
	}

	/**
	 * Represents an acquire lock statement for a aggregate root when only the ID is known.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class AcquireLockRoot<T> implements DbAction<T> {

		private final Object id;
		private final Class<T> entityType;

		AcquireLockRoot(Object id, Class<T> entityType) {
			this.id = id;
			this.entityType = entityType;
		}

		public Object getId() {
			return this.id;
		}

		public Class<T> getEntityType() {
			return this.entityType;
		}

		public String toString() {
			return "DbAction.AcquireLockRoot(id=" + this.getId() + ", entityType=" + this.getEntityType() + ")";
		}
	}

	/**
	 * Represents an acquire lock statement for all aggregate roots of a given type.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	final class AcquireLockAllRoot<T> implements DbAction<T> {

		private final Class<T> entityType;

		AcquireLockAllRoot(Class<T> entityType) {
			this.entityType = entityType;
		}

		public Class<T> getEntityType() {
			return this.entityType;
		}

		public String toString() {
			return "DbAction.AcquireLockAllRoot(entityType=" + this.getEntityType() + ")";
		}
	}

	/**
	 * Represents a batch of {@link DbAction} that share a common value for a property of the action.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 * @since 3.0
	 */
	abstract class BatchWithValue<T, A extends DbAction<T>, B> implements DbAction<T> {

		private final List<A> actions;
		private final B batchValue;

		/**
		 * Creates a {@link BatchWithValue} instance from the given actions and the value which can be extracted by applying
		 * #batchValueExtractor on any of the actions. All actions must result in the same value when #batchValueExtractor
		 * is applied.
		 *
		 * @param actions the actions forming the batch.
		 * @param batchValueExtractor function for extracting the {@link #batchValue} from an action.
		 */
		BatchWithValue(List<A> actions, Function<A, B> batchValueExtractor) {

			Assert.notEmpty(actions, "Actions must contain at least one action");

			Iterator<A> actionIterator = actions.iterator();
			this.batchValue = batchValueExtractor.apply(actionIterator.next());
			actionIterator.forEachRemaining(action -> Assert.isTrue(batchValueExtractor.apply(action).equals(batchValue),
					"All actions in the batch must have matching batchValue"));

			this.actions = actions;
		}

		@Override
		public Class<T> getEntityType() {
			return actions.get(0).getEntityType();
		}

		public List<A> getActions() {
			return actions;
		}

		public B getBatchValue() {
			return batchValue;
		}

		@Override
		public String toString() {
			return "BatchWithValue{" + "actions=" + actions + ", batchValue=" + batchValue + '}';
		}
	}

	/**
	 * Represents a batch insert statement for a multiple entities that are not aggregate roots.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 * @since 2.4
	 */
	final class BatchInsert<T> extends BatchWithValue<T, Insert<T>, IdValueSource> {
		public BatchInsert(List<Insert<T>> actions) {
			super(actions, Insert::getIdValueSource);
		}
	}

	/**
	 * Represents a batch insert statement for a multiple entities that are aggregate roots.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 * @since 3.0
	 */
	final class BatchInsertRoot<T> extends BatchWithValue<T, InsertRoot<T>, IdValueSource> {
		public BatchInsertRoot(List<InsertRoot<T>> actions) {
			super(actions, InsertRoot::getIdValueSource);
		}
	}

	/**
	 * Represents a batch delete statement for multiple entities that are reachable via a given path from the aggregate
	 * root.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 * @since 3.0
	 */
	final class BatchDelete<T>
			extends BatchWithValue<T, Delete<T>, PersistentPropertyPath<RelationalPersistentProperty>> {
		public BatchDelete(List<Delete<T>> actions) {
			super(actions, Delete::getPropertyPath);
		}
	}

	/**
	 * Represents a batch delete statement for multiple entities that are aggregate roots.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 * @since 3.0
	 */
	final class BatchDeleteRoot<T> extends BatchWithValue<T, DeleteRoot<T>, Class<T>> {

		BatchDeleteRoot(List<DeleteRoot<T>> actions) {
			super(actions, DeleteRoot::getEntityType);
		}
	}

	/**
	 * An action depending on another action for providing additional information like the id of a parent entity.
	 *
	 * @author Jens Schauder
	 */
	interface WithDependingOn<T> extends WithPropertyPath<T>, WithEntity<T> {

		/**
		 * The {@link DbAction} of a parent entity, possibly the aggregate root. This is used to obtain values needed to
		 * persist the entity, that are not part of the current entity, especially the id of the parent, which might only
		 * become available once the parent entity got persisted.
		 *
		 * @return guaranteed to be not {@code null}.
		 * @see #getQualifiers()
		 */
		WithEntity<?> getDependingOn();

		/**
		 * Additional values to be set during insert or update statements.
		 * <p>
		 * Values come from parent entities but one might also add values manually.
		 * </p>
		 *
		 * @return guaranteed to be not {@code null}.
		 */
		Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> getQualifiers();

		// TODO: Encapsulate propertyPath and qualifier in object: PropertyPathWithListIndex,
		// PropertyPathWithMapIndex, PropertyPathInSet, PropertyPathWithoutQualifier
		// Probably we need better names.
		@Nullable
		default Pair<PersistentPropertyPath<RelationalPersistentProperty>, Object> getQualifier() {

			Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers = getQualifiers();

			if (qualifiers.isEmpty()) {
				return null;
			}

			Set<Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object>> entries = qualifiers.entrySet();
			Optional<Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object>> optionalEntry = entries.stream()
					.filter(e -> e.getValue() != null).min(Comparator.comparing(e -> -e.getKey().getLength()));

			Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object> entry = optionalEntry.orElse(null);

			if (entry == null) {
				return null;
			}

			return Pair.of(entry.getKey(), entry.getValue());
		}

		@Override
		default Class<T> getEntityType() {
			return WithEntity.super.getEntityType();
		}
	}

	/**
	 * A {@link DbAction} that stores the information of a single entity in the database.
	 *
	 * @author Jens Schauder
	 * @author Chirag Tailor
	 */
	interface WithEntity<T> extends DbAction<T> {

		/**
		 * @return the entity to persist. Guaranteed to be not {@code null}.
		 */
		T getEntity();

		@SuppressWarnings("unchecked")
		@Override
		default Class<T> getEntityType() {
			return (Class<T>) getEntity().getClass();
		}

		/**
		 * @return the {@link IdValueSource} for the entity to persist. Guaranteed to be not {@code null}.
		 */
		IdValueSource getIdValueSource();
	}

	/**
	 * A {@link DbAction} pertaining to the root on an aggregate.
	 *
	 * @author Chirag Tailor
	 */
	interface WithRoot<T> extends WithEntity<T> {
		/**
		 * @param entity the entity to persist. Must not be {@code null}.
		 */
		void setEntity(T entity);
	}

	/**
	 * A {@link DbAction} not operation on the root of an aggregate but on its contained entities.
	 *
	 * @author Jens Schauder
	 */
	interface WithPropertyPath<T> extends DbAction<T> {

		/**
		 * @return the path from the aggregate root to the affected entity
		 */
		PersistentPropertyPath<RelationalPersistentProperty> getPropertyPath();

		@SuppressWarnings("unchecked")
		@Override
		default Class<T> getEntityType() {
			return (Class<T>) getPropertyPath().getLeafProperty().getActualType();
		}
	}
}
