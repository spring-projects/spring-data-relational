/*
 * Copyright 2022-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.DbAction.BatchDelete;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

public class BatchedActionsUnitTests {

	BatchedActions deletes = BatchedActions.batchedDeletes();

	LoggingConsumer consumer = new LoggingConsumer();
	RelationalMappingContext context = new RelationalMappingContext();

	DbAction.Delete<Object> firstOneDelete = new DbAction.Delete(23L, path("one"));
	DbAction.Delete<Object> secondOneDelete = new DbAction.Delete(24L, path("one"));
	DbAction.Delete<Object> firstTwoDelete = new DbAction.Delete(25L, path("two"));
	DbAction.Delete<Object> secondTwoDelete = new DbAction.Delete(26L, path("two"));

	@Test // GH-537
	void emptyBatchedDeletesDoesNotInvokeConsumer() {

		deletes.forEach(consumer);

		assertThat(consumer.log).isEmpty();
	}

	@Test // GH-537
	void singleActionGetsPassedToConsumer() {

		deletes.add(firstOneDelete);

		deletes.forEach(consumer);

		assertThat(consumer.log).containsExactly(firstOneDelete);
	}

	@Test // GH-537
	void multipleUnbatchableActionsGetsPassedToConsumerIndividually() {

		deletes.add(firstOneDelete);
		deletes.add(firstTwoDelete);

		deletes.forEach(consumer);

		assertThat(consumer.log).containsExactlyInAnyOrder(firstOneDelete, firstTwoDelete);
	}

	@Test // GH-537
	void batchableActionsGetPassedToConsumerAsOne() {

		deletes.add(firstOneDelete);
		deletes.add(secondOneDelete);

		deletes.forEach(consumer);

		assertThat(consumer.log).extracting(a -> ((Class)a.getClass())).containsExactly(BatchDelete.class);
	}

	private PersistentPropertyPath<RelationalPersistentProperty> path(String path) {
		return context.getPersistentPropertyPath(path, DummyEntity.class);
	}

	private static class LoggingConsumer implements Consumer<DbAction<?>> {
		List<DbAction<?>> log = new ArrayList<>();

		@Override
		public void accept(DbAction dbAction) {
			log.add(dbAction);
		}
	}

	private static class DummyEntity {
		OtherEntity one;
		OtherEntity two;
	}

	private static class OtherEntity {
		String one;
		String two;
	}

}
