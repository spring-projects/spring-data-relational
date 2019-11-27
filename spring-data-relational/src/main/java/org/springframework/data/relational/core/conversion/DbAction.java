/*
 * Copyright 2018-2019 the original author or authors.
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

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;

/**
 * An instance of this interface represents a (conceptual) single interaction with a database, e.g. a single update,
 * used as a step when synchronizing the state of an aggregate with the database.
 *
 * @param <T> the type of the entity that is affected by this action.
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 */
public interface DbAction<T> {

	Class<T> getEntityType();

	/**
	 * Executing this DbAction with the given {@link Interpreter}.
	 * <p>
	 * The default implementation just performs exception handling and delegates to {@link #doExecuteWith(Interpreter)}.
	 *
	 * @param interpreter the {@link Interpreter} responsible for actually executing the {@link DbAction}.Must not be
	 *          {@code null}.
	 */
	default void executeWith(Interpreter interpreter) {

		try {
			doExecuteWith(interpreter);
		} catch (Exception e) {
			throw new DbActionExecutionException(this, e);
		}
	}

	/**
	 * Executing this DbAction with the given {@link Interpreter} without any exception handling.
	 *
	 * @param interpreter the {@link Interpreter} responsible for actually executing the {@link DbAction}.
	 */
	void doExecuteWith(Interpreter interpreter);

	/**
	 * Represents an insert statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Data
	class Insert<T> implements WithGeneratedId<T>, WithDependingOn<T> {

		@NonNull final T entity;
		@NonNull final PersistentPropertyPath<RelationalPersistentProperty> propertyPath;
		@NonNull final WithEntity<?> dependingOn;

		Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers = new HashMap<>();

		private Object generatedId;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}

		@Override
		public Class<T> getEntityType() {
			return WithDependingOn.super.getEntityType();
		}
	}

	/**
	 * Represents an insert statement for the root of an aggregate. Upon a successful insert, the initial version and generated ids are populated.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Data
	@RequiredArgsConstructor
	class InsertRoot<T> implements WithVersion<T>, WithGeneratedId<T> {

		@NonNull private T entity;
		private Number nextVersion;
		private Object generatedId;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an update statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class Update<T> implements WithEntity<T> {

		@NonNull T entity;
		@NonNull PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an update statement for the aggregate root.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Data
	class UpdateRoot<T> implements WithVersion<T> {

		@NonNull private T entity;
		@Nullable Number nextVersion;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents a merge statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Data
	class Merge<T> implements WithDependingOn<T>, WithPropertyPath<T> {

		@NonNull T entity;
		@NonNull PersistentPropertyPath<RelationalPersistentProperty> propertyPath;
		@NonNull WithEntity<?> dependingOn;

		Map<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifiers = new HashMap<>();

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents a delete statement for all entities that that a reachable via a give path from the aggregate root.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class Delete<T> implements WithPropertyPath<T> {

		@NonNull Object rootId;
		@NonNull PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents a delete statement for a aggregate root when only the ID is known.
	 * <p>
	 * Note that deletes for contained entities that reference the root are to be represented by separate
	 * {@link DbAction}s.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class DeleteRoot<T> implements WithEntity<T> {

		@NonNull Object id;
		@Nullable T entity;
		@NonNull Class<T> entityType;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an delete statement for all entities that that a reachable via a give path from any aggregate root of a
	 * given type.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class DeleteAll<T> implements WithPropertyPath<T> {

		@NonNull PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents a delete statement for all aggregate roots of a given type.
	 * <p>
	 * Note that deletes for contained entities that reference the root are to be represented by separate
	 * {@link DbAction}s.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class DeleteAllRoot<T> implements DbAction<T> {

		@NonNull private final Class<T> entityType;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
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
			if (qualifiers.size() == 0)
				return null;

			if (qualifiers.size() > 1) {
				throw new IllegalStateException("Can't handle more then on qualifier");
			}

			Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object> entry = qualifiers.entrySet().iterator()
					.next();
			if (entry.getValue() == null) {
				return null;
			}
			return Pair.of(entry.getKey(), entry.getValue());
		};

		@Override
		default Class<T> getEntityType() {
			return WithEntity.super.getEntityType();
		}
	}

	/**
	 * A {@link DbAction} that stores the information of a single entity in the database.
	 *
	 * @author Jens Schauder
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
	}

	/**
	 * A {@link DbAction} that may "update" its entity. In order to support immutable entities this requires at least
	 * potentially creating a new instance, which this interface makes available.
	 *
	 * @author Jens Schauder
	 */
	interface WithGeneratedId<T> extends WithEntity<T> {

		/**
		 * @return the entity to persist. Guaranteed to be not {@code null}.
		 */
		@Nullable
		Object getGeneratedId();

		@SuppressWarnings("unchecked")
		@Override
		default Class<T> getEntityType() {
			return (Class<T>) getEntity().getClass();
		}
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
			return (Class<T>) getPropertyPath().getRequiredLeafProperty().getActualType();
		}
	}
	interface WithVersion<T> extends WithEntity<T> {
		Number getNextVersion();
		void setNextVersion(Number nextVersion);
	}
}
