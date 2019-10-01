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

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An instance of this interface represents a (conceptual) single interaction with a database, e.g. a single update,
 * used as a step when synchronizing the state of an aggregate with the database.
 *
 * @param <T> the type of the entity that is affected by this action.
 * @author Jens Schauder
 * @author Mark Paluch
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
	abstract class InsertLike<T> extends WithIdentifier<T> implements WithDependingOn<T> {

		@NonNull final T entity;
		@NonNull final PersistentPropertyPath<RelationalPersistentProperty> propertyPath;
		@NonNull final WithIdentifier<?> dependingOn;

		@Getter(value = AccessLevel.NONE) private final IdentifierContext identifierContext;

		private final Object qualifier;

		private Object generatedId;

		public InsertLike(T entity, PersistentPropertyPath<RelationalPersistentProperty> propertyPath,
				WithIdentifier<?> dependingOn) {
			this(entity, propertyPath, dependingOn, null);
		}

		public InsertLike(T entity, PersistentPropertyPath<RelationalPersistentProperty> propertyPath,
				WithIdentifier<?> dependingOn, @Nullable Object qualifier) {

			this.entity = entity;
			this.propertyPath = propertyPath;
			this.dependingOn = dependingOn;
			this.qualifier = qualifier;

			IdentifierContext parentIdContext;
			if (dependingOn instanceof WithIdentifier) {
				parentIdContext = ((WithIdentifier<?>) dependingOn).getIdentifierContext();
			} else {
				throw new IllegalArgumentException("dependingOn must be either an InsertRoot or an Insert");
			}

			Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id = //
					propertyPath.getRequiredLeafProperty().getRequiredPersistentEntity().hasIdProperty() //
							? this::getActualId //
							: null;
			identifierContext = parentIdContext.withQualifier(propertyPath, qualifier, id);
		}

		@Override
		public Class<T> getEntityType() {
			return WithDependingOn.super.getEntityType();
		}

		@Override
		protected IdentifierContext getIdentifierContext() {
			return identifierContext;
		}

		@Override
		public Identifier getIdentifier(RelationalMappingContext context,
				PersistentPropertyPath<RelationalPersistentProperty> path) {
			return identifierContext.toIdentifier(context, path);
		}

		protected Object getActualId(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context) {

			if (getGeneratedId() != null) {
				return getGeneratedId();
			}
			RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(getEntityType());
			return persistentEntity.getIdentifierAccessor(getEntity()).getIdentifier();
		}
	}

	class Insert<T> extends InsertLike<T> {
		public Insert(T entity, PersistentPropertyPath propertyPath, WithIdentifier dependingOn) {
			super(entity, propertyPath, dependingOn);
		}

		public Insert(T entity, PersistentPropertyPath propertyPath, WithIdentifier dependingOn, Object qualifier) {
			super(entity, propertyPath, dependingOn, qualifier);
		}

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}

	}

	/**
	 * Represents an insert statement for the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class InsertRoot<T> extends WithIdentifier<T> implements WithGeneratedId<T> {

		private final Supplier<T> entitySupplier;

		private Object generatedId;

		public InsertRoot(Supplier<T> entitySupplier) {
			this.entitySupplier = entitySupplier;
		}

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}

		protected Object getActualId(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context) {

			if (getGeneratedId() != null) {
				return getGeneratedId();
			}
			RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(getEntityType());
			return persistentEntity.getIdentifierAccessor(getEntity()).getIdentifier();
		}

		@Override
		public T getEntity() {
			return (entitySupplier.get());
		}

		public void setGeneratedId(Object id) {
			generatedId = id;
		}

		@Override
		public Object getGeneratedId() {
			return generatedId;
		}

		public String toString() {
			return "DbAction.InsertRoot(entity=" + entitySupplier.get() + ", generatedId=" + this.getGeneratedId() + ")";
		}
	}

	/**
	 * Represents an update statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class Update<T> implements WithEntity<T>, WithPropertyPath<T> {

		@NonNull T entity;
		@NonNull PersistentPropertyPath<RelationalPersistentProperty> propertyPath;

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}

		@Override
		public Class<T> getEntityType() {
			return WithEntity.super.getEntityType();
		}
	}

	/**
	 * Represents an insert statement for the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class UpdateRoot<T> extends WithIdentifier<T> implements WithEntity<T> {

		private final Supplier<T> entitySupplier;

		public UpdateRoot(Supplier<T> entitySupplier) {
			this.entitySupplier = entitySupplier;
		}

		@Override
		public void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}

		@Override
		public T getEntity() {
			return entitySupplier.get();
		}

		public String toString() {
			return "DbAction.UpdateRoot(entity=" + this.entitySupplier.get() + ")";
		}
	}

	/**
	 * Represents a merge statement for a single entity that is not the root of an aggregate.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	class Merge<T> extends InsertLike<T> {

		public Merge(T entity, PersistentPropertyPath<RelationalPersistentProperty> propertyPath,
				WithIdentifier<?> dependingOn) {
			super(entity, propertyPath, dependingOn);
		}

		public Merge(T entity, PersistentPropertyPath<RelationalPersistentProperty> propertyPath,
				WithIdentifier<?> dependingOn, Object qualifier) {
			super(entity, propertyPath, dependingOn, qualifier);
		}

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
	 * Represents a delete statement for a aggregate root.
	 * <p>
	 * Note that deletes for contained entities that reference the root are to be represented by separate
	 * {@link DbAction}s.
	 *
	 * @param <T> type of the entity for which this represents a database interaction.
	 */
	@Value
	class DeleteRoot<T> implements DbAction<T> {

		@NonNull Class<T> entityType;
		@NonNull Object rootId;

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
	interface WithDependingOn<T> extends WithPropertyPath<T>, WithGeneratedId<T> {

		/**
		 * The {@link DbAction} of a parent entity, possibly the aggregate root. This is used to obtain values needed to
		 * persist the entity, that are not part of the current entity, especially the id of the parent, which might only
		 * become available once the parent entity got persisted.
		 *
		 * @return guaranteed to be not {@code null}.
		 */
		WithIdentifier<?> getDependingOn();

		@Override
		default Class<T> getEntityType() {
			return WithGeneratedId.super.getEntityType();
		}

		Object getQualifier();

		Identifier getIdentifier(RelationalMappingContext context,
				PersistentPropertyPath<RelationalPersistentProperty> path);
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

	abstract class WithIdentifier<T> implements WithEntity<T> {
		private final IdentifierContext identifierContext = IdentifierContext.of(context -> getActualId(context));

		protected IdentifierContext getIdentifierContext() {
			return identifierContext;
		}

		protected Object getActualId(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context) {

			RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(getEntityType());
			Object id = persistentEntity.getIdentifierAccessor(getEntity()).getIdentifier();

			Assert.state(id != null, "Id must not be null");

			return id;
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
}
