/*
 * Copyright 2017-2024 the original author or authors.
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

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.relational.core.conversion.DbAction.AcquireLockAllRoot;
import org.springframework.data.relational.core.conversion.DbAction.AcquireLockRoot;
import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAll;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAllRoot;
import org.springframework.data.relational.core.conversion.DbAction.DeleteRoot;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the {@link org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter}
 *
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
@ExtendWith(MockitoExtension.class)
public class RelationalEntityDeleteWriterUnitTests {

	RelationalEntityDeleteWriter converter = new RelationalEntityDeleteWriter(new RelationalMappingContext());

	@Test // DATAJDBC-112
	public void deleteDeletesTheEntityAndReferencedEntities() {

		SomeEntity entity = new SomeEntity(23L);

		MutableAggregateChange<SomeEntity> aggregateChange = MutableAggregateChange.forDelete(SomeEntity.class);

		converter.write(entity.id, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly( //
						Tuple.tuple(AcquireLockRoot.class, SomeEntity.class, ""), //
						Tuple.tuple(Delete.class, YetAnother.class, "other.yetAnother"), //
						Tuple.tuple(Delete.class, OtherEntity.class, "other"), //
						Tuple.tuple(DeleteRoot.class, SomeEntity.class, "") //
				);
	}

	@Test // DATAJDBC-493
	public void deleteDeletesTheEntityAndNoReferencedEntities() {

		SingleEntity entity = new SingleEntity(23L);

		MutableAggregateChange<SingleEntity> aggregateChange = MutableAggregateChange.forDelete(SingleEntity.class);

		converter.write(entity.id, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly(Tuple.tuple(DeleteRoot.class, SingleEntity.class, ""));
	}

	@Test // DATAJDBC-188
	public void deleteAllDeletesAllEntitiesAndReferencedEntities() {

		MutableAggregateChange<SomeEntity> aggregateChange = MutableAggregateChange.forDelete(SomeEntity.class);

		converter.write(null, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly( //
						Tuple.tuple(AcquireLockAllRoot.class, SomeEntity.class, ""), //
						Tuple.tuple(DeleteAll.class, YetAnother.class, "other.yetAnother"), //
						Tuple.tuple(DeleteAll.class, OtherEntity.class, "other"), //
						Tuple.tuple(DeleteAllRoot.class, SomeEntity.class, "") //
				);
	}

	@Test // DATAJDBC-493
	public void deleteAllDeletesAllEntitiesAndNoReferencedEntities() {

		MutableAggregateChange<SingleEntity> aggregateChange = MutableAggregateChange.forDelete(SingleEntity.class);

		converter.write(null, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly(Tuple.tuple(DeleteAllRoot.class, SingleEntity.class, ""));
	}

	@Test // GH-1249
	public void deleteDoesNotDeleteReadOnlyReferences() {

		WithReadOnlyReference entity = new WithReadOnlyReference(23L);

		MutableAggregateChange<WithReadOnlyReference> aggregateChange = MutableAggregateChange
				.forDelete(WithReadOnlyReference.class);

		converter.write(entity.id, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly( //
						Tuple.tuple(DeleteRoot.class, WithReadOnlyReference.class, "") //
				);
	}

	@Test // GH-1249
	public void deleteAllDoesNotDeleteReadOnlyReferences() {

		WithReadOnlyReference entity = new WithReadOnlyReference(23L);

		MutableAggregateChange<WithReadOnlyReference> aggregateChange = MutableAggregateChange
				.forDelete(WithReadOnlyReference.class);

		converter.write(null, aggregateChange);

		assertThat(extractActions(aggregateChange))
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath) //
				.containsExactly( //
						Tuple.tuple(DeleteAllRoot.class, WithReadOnlyReference.class, "") //
				);
	}

	private List<DbAction<?>> extractActions(MutableAggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();
		aggregateChange.forEachAction(actions::add);
		return actions;
	}

	private static class SomeEntity {

		@Id final Long id;
		OtherEntity other;
		// should not trigger own Dbaction
		String name;

		private SomeEntity(Long id) {
			this.id = id;
		}
	}

	private class OtherEntity {

		@Id final Long id;
		YetAnother yetAnother;

		private OtherEntity(Long id) {
			this.id = id;
		}
	}

	private class YetAnother {
		@Id final Long id;

		private YetAnother(Long id) {
			this.id = id;
		}
	}

	private class SingleEntity {
		@Id final Long id;
		String name;

		private SingleEntity(Long id) {
			this.id = id;
		}
	}

	private static class WithReadOnlyReference {

		@Id
		final Long id;
		@ReadOnlyProperty
		OtherEntity other;

		public WithReadOnlyReference(Long id) {
			this.id = id;
		}
	}
}
