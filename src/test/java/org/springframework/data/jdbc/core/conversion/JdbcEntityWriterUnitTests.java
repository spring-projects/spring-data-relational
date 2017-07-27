/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.conversion.AggregateChange.Kind;
import org.springframework.data.jdbc.core.conversion.DbAction.Delete;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;

/**
 * Unit tests for the {@link JdbcEntityWriter}
 * 
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcEntityWriterUnitTests {

	JdbcEntityWriter converter = new JdbcEntityWriter(new JdbcMappingContext());

	@Test // DATAJDBC-112
	public void newEntityGetsConvertedToOneInsert() {

		SomeEntity entity = new SomeEntity(null);
		AggregateChange<SomeEntity> aggregateChange = new AggregateChange(Kind.SAVE, SomeEntity.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Insert.class, SomeEntity.class) //
		);
	}

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToUpdate() {

		SomeEntity entity = new SomeEntity(23L);

		AggregateChange<SomeEntity> aggregateChange = new AggregateChange(Kind.SAVE, SomeEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Delete.class, OtherEntity.class), //
						tuple(Update.class, SomeEntity.class) //
		);
	}

	@Test // DATAJDBC-112
	public void referenceTriggersDeletePlusInsert() {

		SomeEntity entity = new SomeEntity(23L);
		entity.setOther(new OtherEntity(null));

		AggregateChange<SomeEntity> aggregateChange = new AggregateChange(Kind.SAVE, SomeEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Delete.class, OtherEntity.class), //
						tuple(Update.class, SomeEntity.class), //
						tuple(Insert.class, OtherEntity.class) //
		);
	}

	@Data
	private static class SomeEntity {

		@Id final Long id;
		OtherEntity other;
		// should not trigger own Dbaction
		String name;
	}

	@Data
	private class OtherEntity {
		@Id final Long id;
	}
}
