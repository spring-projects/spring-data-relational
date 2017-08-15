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

import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.conversion.AggregateChange.Kind;
import org.springframework.data.jdbc.core.conversion.DbAction.Delete;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.mapping.model.DefaultNamingStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;

/**
 * Unit tests for the {@link JdbcEntityWriter}
 * 
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcEntityWriterUnitTests {

	JdbcEntityWriter converter = new JdbcEntityWriter(new JdbcMappingContext(new DefaultNamingStrategy()));

	@Test // DATAJDBC-112
	public void newEntityGetsConvertedToOneInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Insert.class, SingleReferenceEntity.class) //
		);
	}

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToUpdate() {

		SingleReferenceEntity entity = new SingleReferenceEntity(23L);
		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Delete.class, Element.class), //
						tuple(Update.class, SingleReferenceEntity.class) //
		);
	}

	@Test // DATAJDBC-112
	public void referenceTriggersDeletePlusInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(23L);
		entity.other = new Element(null);

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SingleReferenceEntity.class,
				entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Delete.class, Element.class), //
						tuple(Update.class, SingleReferenceEntity.class), //
						tuple(Insert.class, Element.class) //
		);
	}

	@Test // DATAJDBC-113
	public void newEntityWithEmptySetResultsInSingleInsert() {

		SetContainer entity = new SetContainer(null);
		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SetContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Insert.class, SetContainer.class));
	}

	@Test // DATAJDBC-113
	public void newEntityWithSetResultsInAdditionalInsertPerElement() {

		SetContainer entity = new SetContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SetContainer.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Insert.class, SetContainer.class), //
						tuple(Insert.class, Element.class), //
						tuple(Insert.class, Element.class) //
		);
	}

	@Test // DATAJDBC-113
	public void cascadingReferencesTriggerCascadingActions() {

		CascadingReferenceEntity entity = new CascadingReferenceEntity(null);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SetContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						tuple(Insert.class, CascadingReferenceEntity.class), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class), //
						tuple(Insert.class, Element.class), //
						tuple(Insert.class, Element.class), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class), //
						tuple(Insert.class, Element.class), //
						tuple(Insert.class, Element.class) //
		);
	}

	private CascadingReferenceMiddleElement createMiddleElement(Element first, Element second) {

		CascadingReferenceMiddleElement middleElement1 = new CascadingReferenceMiddleElement(null);
		middleElement1.element.add(first);
		middleElement1.element.add(second);
		return middleElement1;
	}

	@RequiredArgsConstructor
	static class SingleReferenceEntity {

		@Id final Long id;
		Element other;
		// should not trigger own Dbaction
		String name;
	}

	@RequiredArgsConstructor
	private static class CascadingReferenceMiddleElement {

		@Id final Long id;
		final Set<Element> element = new HashSet<>();
	}

	@RequiredArgsConstructor
	private static class CascadingReferenceEntity {

		@Id final Long id;
		final Set<CascadingReferenceMiddleElement> other = new HashSet<>();
	}

	@RequiredArgsConstructor
	private static class SetContainer {

		@Id final Long id;
		Set<Element> elements = new HashSet<>();
	}

	@RequiredArgsConstructor
	private static class Element {
		@Id final Long id;
	}

}
