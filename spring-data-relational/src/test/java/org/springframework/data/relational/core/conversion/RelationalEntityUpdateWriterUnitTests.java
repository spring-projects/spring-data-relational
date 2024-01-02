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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for the {@link RelationalEntityUpdateWriter}
 *
 * @author Thomas Lang
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
@ExtendWith(MockitoExtension.class)
public class RelationalEntityUpdateWriterUnitTests {

	public static final long SOME_ENTITY_ID = 23L;
	private final RelationalMappingContext context = new RelationalMappingContext();

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToDeletePlusUpdate() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID, null, null);

		RootAggregateChange<SingleReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityUpdateWriter<SingleReferenceEntity>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath,
						DbActionTestSupport::actualEntityType, DbActionTestSupport::isWithDependsOn) //
				.containsExactly( //
						tuple(DbAction.UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false), //
						tuple(DbAction.Delete.class, Element.class, "other", null, false) //
				);
	}

	private List<DbAction<?>> extractActions(MutableAggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();
		aggregateChange.forEachAction(actions::add);
		return actions;
	}

	record SingleReferenceEntity(

			@Id Long id, Element other,
			// should not trigger own Dbaction
			String name) {
	}

	record Element(@Id Long id) {
	}

}
