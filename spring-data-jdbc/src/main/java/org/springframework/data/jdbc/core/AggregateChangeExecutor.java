/*
 * Copyright 2020-2021 the original author or authors.
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

import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.lang.Nullable;

/**
 * Executes an {@link MutableAggregateChange}.
 *
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 2.0
 */
class AggregateChangeExecutor {

	private final JdbcConverter converter;
	private final DataAccessStrategy accessStrategy;

	AggregateChangeExecutor(JdbcConverter converter, DataAccessStrategy accessStrategy) {

		this.converter = converter;
		this.accessStrategy = accessStrategy;
	}

	@Nullable
	<T> T execute(AggregateChange<T> aggregateChange) {

		JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
				accessStrategy);

		aggregateChange.forEachAction(action -> execute(action, executionContext));

		T root = executionContext.populateIdsIfNecessary();
		root = root == null ? aggregateChange.getEntity() : root;

		if (root != null) {
			root = executionContext.populateRootVersionIfNecessary(root);
		}

		return root;
	}

	private void execute(DbAction<?> action, JdbcAggregateChangeExecutionContext executionContext) {

		try {
			if (action instanceof DbAction.InsertRoot) {
				executionContext.executeInsertRoot((DbAction.InsertRoot<?>) action);
			} else if (action instanceof DbAction.Insert) {
				executionContext.executeInsert((DbAction.Insert<?>) action);
			} else if (action instanceof DbAction.UpdateRoot) {
				executionContext.executeUpdateRoot((DbAction.UpdateRoot<?>) action);
			} else if (action instanceof DbAction.Update) {
				executionContext.executeUpdate((DbAction.Update<?>) action);
			} else if (action instanceof DbAction.Delete) {
				executionContext.executeDelete((DbAction.Delete<?>) action);
			} else if (action instanceof DbAction.DeleteAll) {
				executionContext.executeDeleteAll((DbAction.DeleteAll<?>) action);
			} else if (action instanceof DbAction.DeleteRoot) {
				executionContext.executeDeleteRoot((DbAction.DeleteRoot<?>) action);
			} else if (action instanceof DbAction.DeleteAllRoot) {
				executionContext.executeDeleteAllRoot((DbAction.DeleteAllRoot<?>) action);
			} else if (action instanceof DbAction.AcquireLockRoot) {
				executionContext.executeAcquireLock((DbAction.AcquireLockRoot<?>) action);
			} else if (action instanceof DbAction.AcquireLockAllRoot) {
				executionContext.executeAcquireLockAllRoot((DbAction.AcquireLockAllRoot<?>) action);
			} else {
				throw new RuntimeException("unexpected action");
			}
		} catch (Exception e) {
			throw new DbActionExecutionException(action, e);
		}
	}
}
