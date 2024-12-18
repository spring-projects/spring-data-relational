/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.util.List;
import java.util.Optional;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;

/**
 * Executes an {@link MutableAggregateChange}.
 *
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @since 2.0
 */
class AggregateChangeExecutor {

	private final JdbcConverter converter;
	private final DataAccessStrategy accessStrategy;
	private final PersistenceExceptionTranslator jdbcExceptionTranslator;

	AggregateChangeExecutor(JdbcConverter converter, DataAccessStrategy accessStrategy) {

		this.converter = converter;
		this.accessStrategy = accessStrategy;
		this.jdbcExceptionTranslator = new JdbcExceptionTranslator();
	}

	/**
	 * Execute a save aggregate change. It returns the resulting root entities, with all changes that might apply. This
	 * might be the original instances or new instances, depending on their mutability.
	 * 
	 * @param aggregateChange the aggregate change to be executed. Must not be {@literal null}.
	 * @param <T> the type of the aggregate root.
	 * @return the aggregate roots resulting from the change, if there are any. May be empty.
	 * @since 3.0
	 */
	<T> List<T> executeSave(AggregateChange<T> aggregateChange) {

		JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
				accessStrategy);

		aggregateChange.forEachAction(action -> execute(action, executionContext));

		return executionContext.populateIdsIfNecessary();
	}

	/**
	 * Execute a delete aggregate change.
	 *
	 * @param aggregateChange the aggregate change to be executed. Must not be {@literal null}.
	 * @param <T> the type of the aggregate root.
	 * @since 3.0
	 */
	<T> void executeDelete(AggregateChange<T> aggregateChange) {

		JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
				accessStrategy);

		aggregateChange.forEachAction(action -> execute(action, executionContext));
	}

	private void execute(DbAction<?> action, JdbcAggregateChangeExecutionContext executionContext) {

		try {
			if (action instanceof DbAction.InsertRoot<?> insertRoot) {
				executionContext.executeInsertRoot(insertRoot);
			} else if (action instanceof DbAction.BatchInsertRoot<?> batchInsertRoot) {
				executionContext.executeBatchInsertRoot(batchInsertRoot);
			} else if (action instanceof DbAction.Insert<?> insert) {
				executionContext.executeInsert(insert);
			} else if (action instanceof DbAction.BatchInsert<?> batchInsert) {
				executionContext.executeBatchInsert(batchInsert);
			} else if (action instanceof DbAction.UpdateRoot<?> updateRoot) {
				executionContext.executeUpdateRoot(updateRoot);
			} else if (action instanceof DbAction.Delete<?> delete) {
				executionContext.executeDelete(delete);
			} else if (action instanceof DbAction.BatchDelete<?> batchDelete) {
				executionContext.executeBatchDelete(batchDelete);
			} else if (action instanceof DbAction.DeleteAll<?> deleteAll) {
				executionContext.executeDeleteAll(deleteAll);
			} else if (action instanceof DbAction.DeleteRoot<?> deleteRoot) {
				executionContext.executeDeleteRoot(deleteRoot);
			} else if (action instanceof DbAction.BatchDeleteRoot<?> batchDeleteRoot) {
				executionContext.executeBatchDeleteRoot(batchDeleteRoot);
			} else if (action instanceof DbAction.DeleteAllRoot<?> deleteAllRoot) {
				executionContext.executeDeleteAllRoot(deleteAllRoot);
			} else if (action instanceof DbAction.AcquireLockRoot<?> acquireLockRoot) {
				executionContext.executeAcquireLock(acquireLockRoot);
			} else if (action instanceof DbAction.AcquireLockAllRoot<?> acquireLockAllRoot) {
				executionContext.executeAcquireLockAllRoot(acquireLockAllRoot);
			} else {
				throw new RuntimeException("unexpected action");
			}
		} catch (RuntimeException e) {

			throw Optional
					.ofNullable(jdbcExceptionTranslator.translateExceptionIfPossible(e))
					.map(it -> (RuntimeException) it)
					.orElseGet(() -> {
						e.addSuppressed(new DbActionExecutionException(action, e));
						return e;
					});
		}
	}
}
