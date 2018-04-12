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
package org.springframework.data.jdbc.core.conversion;

import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.Assert;

/**
 * Abstracts over a single interaction with a database.
 *
 * @author Jens Schauder
 * @since 1.0
 */
@ToString
@Getter
public abstract class DbAction<T> {

	/**
	 * {@link Class} of the entity of which the database representation is affected by this action.
	 */
	private final Class<T> entityType;

	/**
	 * The entity of which the database representation is affected by this action. Might be {@literal null}.
	 */
	private final T entity;

	/**
	 * The path from the Aggregate Root to the entity affected by this {@link DbAction}.
	 */
	private final JdbcPropertyPath propertyPath;

	/**
	 * Key-value-pairs to specify additional values to be used with the statement which can't be obtained from the entity,
	 * nor from {@link DbAction}s {@literal this} depends on. A used case are map keys, which need to be persisted with
	 * the map value but aren't part of the value.
	 */
	private final Map<String, Object> additionalValues = new HashMap<>();

	/**
	 * Another action, this action depends on. For example the insert for one entity might need the id of another entity,
	 * which gets insert before this one. That action would be referenced by this property, so that the id becomes
	 * available at execution time. Might be {@literal null}.
	 */
	private final DbAction dependingOn;

	private DbAction(Class<T> entityType, T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {

		this.entityType = entityType;
		this.entity = entity;
		this.propertyPath = propertyPath;
		this.dependingOn = dependingOn;
	}

	public static <T> Insert<T> insert(T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {
		return new Insert<>(entity, propertyPath, dependingOn);
	}

	public static <T> Update<T> update(T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {
		return new Update<>(entity, propertyPath, dependingOn);
	}

	public static <T> Delete<T> delete(Object id, Class<T> type, T entity, JdbcPropertyPath propertyPath,
			DbAction dependingOn) {
		return new Delete<>(id, type, entity, propertyPath, dependingOn);
	}

	public static <T> DeleteAll<T> deleteAll(Class<T> type, JdbcPropertyPath propertyPath, DbAction dependingOn) {
		return new DeleteAll<>(type, propertyPath, dependingOn);
	}

	/**
	 * Executing this DbAction with the given {@link Interpreter}.
	 *
	 * @param interpreter the {@link Interpreter} responsible for actually executing the {@link DbAction}.
	 */
	void executeWith(Interpreter interpreter) {

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
	protected abstract void doExecuteWith(Interpreter interpreter);

	/**
	 * {@link InsertOrUpdate} must reference an entity.
	 *
	 * @param <T> type o the entity for which this represents a database interaction
	 */
	abstract static class InsertOrUpdate<T> extends DbAction<T> {

		@SuppressWarnings("unchecked")
		InsertOrUpdate(T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {
			super((Class<T>) entity.getClass(), entity, propertyPath, dependingOn);
		}
	}

	/**
	 * Represents an insert statement.
	 *
	 * @param <T> type o the entity for which this represents a database interaction
	 */
	public static class Insert<T> extends InsertOrUpdate<T> {

		private Insert(T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {
			super(entity, propertyPath, dependingOn);
		}

		@Override
		protected void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an update statement.
	 *
	 * @param <T> type o the entity for which this represents a database interaction
	 */
	public static class Update<T> extends InsertOrUpdate<T> {

		private Update(T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {
			super(entity, propertyPath, dependingOn);
		}

		@Override
		protected void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an delete statement, possibly a cascading delete statement, i.e. the delete necessary because one
	 * aggregate root gets deleted.
	 *
	 * @param <T> type o the entity for which this represents a database interaction
	 */
	@Getter
	public static class Delete<T> extends DbAction<T> {

		/**
		 * Id of the root for which all via {@link #propertyPath} referenced entities shall get deleted
		 */
		private final Object rootId;

		private Delete(Object rootId, Class<T> type, T entity, JdbcPropertyPath propertyPath, DbAction dependingOn) {

			super(type, entity, propertyPath, dependingOn);

			Assert.notNull(rootId, "rootId must not be null.");

			this.rootId = rootId;
		}

		@Override
		protected void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}

	/**
	 * Represents an delete statement.
	 *
	 * @param <T> type o the entity for which this represents a database interaction
	 */
	public static class DeleteAll<T> extends DbAction<T> {

		private DeleteAll(Class<T> entityType, JdbcPropertyPath propertyPath, DbAction dependingOn) {
			super(entityType, null, propertyPath, dependingOn);
		}

		@Override
		protected void doExecuteWith(Interpreter interpreter) {
			interpreter.interpret(this);
		}
	}
}
