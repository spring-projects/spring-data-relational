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

import static org.assertj.core.api.Assertions.*;

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.conversion.AggregateChange.Kind;
import org.springframework.data.jdbc.core.conversion.DbAction.Delete;
import org.springframework.data.jdbc.core.conversion.DbAction.DeleteAll;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;

/**
 * Unit tests for the {@link JdbcEntityWriter}
 *
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcEntityWriterUnitTests {

	public static final long SOME_ENTITY_ID = 23L;
	JdbcEntityWriter converter = new JdbcEntityWriter(new JdbcMappingContext());

	@Test // DATAJDBC-112
	public void newEntityGetsConvertedToOneInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, SingleReferenceEntity.class, "") //
				);
	}

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToUpdate() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);
		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Delete.class, Element.class, "other"), //
						tuple(Update.class, SingleReferenceEntity.class, "") //
				);
	}

	@Test // DATAJDBC-112
	public void referenceTriggersDeletePlusInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);
		entity.other = new Element(null);

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SingleReferenceEntity.class,
				entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Delete.class, Element.class, "other"), //
						tuple(Update.class, SingleReferenceEntity.class, ""), //
						tuple(Insert.class, Element.class, "other") //
				);
	}

	@Test // DATAJDBC-113
	public void newEntityWithEmptySetResultsInSingleInsert() {

		SetContainer entity = new SetContainer(null);
		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SetContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, SetContainer.class, ""));
	}

	@Test // DATAJDBC-113
	public void newEntityWithSetResultsInAdditionalInsertPerElement() {

		SetContainer entity = new SetContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, SetContainer.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, SetContainer.class, ""), //
						tuple(Insert.class, Element.class, "elements"), //
						tuple(Insert.class, Element.class, "elements") //
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

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, CascadingReferenceEntity.class, ""), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, Element.class, "other.element") //
				);
	}

	@Test // DATAJDBC-188
	public void cascadingReferencesTriggerCascadingActionsForUpdate() {

		CascadingReferenceEntity entity = new CascadingReferenceEntity(23L);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange(Kind.SAVE, CascadingReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Delete.class, Element.class, "other.element"),
						tuple(Delete.class, CascadingReferenceMiddleElement.class, "other"),
						tuple(Update.class, CascadingReferenceEntity.class, ""), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other"), //
						tuple(Insert.class, Element.class, "other.element"), //
						tuple(Insert.class, Element.class, "other.element") //
		);
	}

	@Test // DATAJDBC-131
	public void newEntityWithEmptyMapResultsInSingleInsert() {

		MapContainer entity = new MapContainer(null);
		AggregateChange<MapContainer> aggregateChange = new AggregateChange(Kind.SAVE, MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, MapContainer.class, ""));
	}

	@Test // DATAJDBC-131
	public void newEntityWithMapResultsInAdditionalInsertPerElement() {

		MapContainer entity = new MapContainer(null);
		entity.elements.put("one", new Element(null));
		entity.elements.put("two", new Element(null));

		AggregateChange<MapContainer> aggregateChange = new AggregateChange(Kind.SAVE, MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions())
				.extracting(DbAction::getClass, DbAction::getEntityType, this::getMapKey, this::extractPath) //
				.containsExactlyInAnyOrder( //
						tuple(Insert.class, MapContainer.class, null, ""), //
						tuple(Insert.class, Element.class, "one", "elements"), //
						tuple(Insert.class, Element.class, "two", "elements") //
				).containsSubsequence( // container comes before the elements
						tuple(Insert.class, MapContainer.class, null, ""), //
						tuple(Insert.class, Element.class, "two", "elements") //
				).containsSubsequence( // container comes before the elements
						tuple(Insert.class, MapContainer.class, null, ""), //
						tuple(Insert.class, Element.class, "one", "elements") //
				);
	}

	@Test // DATAJDBC-183
	public void newEntityWithFullMapResultsInAdditionalInsertPerElement() {

		MapContainer entity = new MapContainer(null);
		entity.elements.put("1", new Element(null));
		entity.elements.put("2", new Element(null));
		entity.elements.put("3", new Element(null));
		entity.elements.put("4", new Element(null));
		entity.elements.put("5", new Element(null));
		entity.elements.put("6", new Element(null));
		entity.elements.put("7", new Element(null));
		entity.elements.put("8", new Element(null));
		entity.elements.put("9", new Element(null));
		entity.elements.put("0", new Element(null));
		entity.elements.put("a", new Element(null));
		entity.elements.put("b", new Element(null));

		AggregateChange<MapContainer> aggregateChange = new AggregateChange(Kind.SAVE, MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions())
				.extracting(DbAction::getClass, DbAction::getEntityType, this::getMapKey, this::extractPath) //
				.containsExactlyInAnyOrder( //
						tuple(Insert.class, MapContainer.class, null, ""), //
						tuple(Insert.class, Element.class, "1", "elements"), //
						tuple(Insert.class, Element.class, "2", "elements"), //
						tuple(Insert.class, Element.class, "3", "elements"), //
						tuple(Insert.class, Element.class, "4", "elements"), //
						tuple(Insert.class, Element.class, "5", "elements"), //
						tuple(Insert.class, Element.class, "6", "elements"), //
						tuple(Insert.class, Element.class, "7", "elements"), //
						tuple(Insert.class, Element.class, "8", "elements"), //
						tuple(Insert.class, Element.class, "9", "elements"), //
						tuple(Insert.class, Element.class, "0", "elements"), //
						tuple(Insert.class, Element.class, "a", "elements"), //
						tuple(Insert.class, Element.class, "b", "elements") //
		);
	}

	@Test // DATAJDBC-130
	public void newEntityWithEmptyListResultsInSingleInsert() {

		ListContainer entity = new ListContainer(null);
		AggregateChange<ListContainer> aggregateChange = new AggregateChange(Kind.SAVE, ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath) //
				.containsExactly( //
						tuple(Insert.class, ListContainer.class, ""));
	}

	@Test // DATAJDBC-130
	public void newEntityWithListResultsInAdditionalInsertPerElement() {

		ListContainer entity = new ListContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		AggregateChange<ListContainer> aggregateChange = new AggregateChange(Kind.SAVE, ListContainer.class, entity);
		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions())
				.extracting(DbAction::getClass, DbAction::getEntityType, this::getListKey, this::extractPath) //
				.containsExactlyInAnyOrder( //
						tuple(Insert.class, ListContainer.class, null, ""), //
						tuple(Insert.class, Element.class, 0, "elements"), //
						tuple(Insert.class, Element.class, 1, "elements") //
				).containsSubsequence( // container comes before the elements
						tuple(Insert.class, ListContainer.class, null, ""), //
						tuple(Insert.class, Element.class, 1, "elements") //
				).containsSubsequence( // container comes before the elements
						tuple(Insert.class, ListContainer.class, null, ""), //
						tuple(Insert.class, Element.class, 0, "elements") //
				);
	}

	@Test // DATAJDBC-131
	public void mapTriggersDeletePlusInsert() {

		MapContainer entity = new MapContainer(SOME_ENTITY_ID);
		entity.elements.put("one", new Element(null));

		AggregateChange<MapContainer> aggregateChange = new AggregateChange(Kind.SAVE, MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::getMapKey, this::extractPath) //
				.containsExactly( //
						tuple(Delete.class, Element.class, null, "elements"), //
						tuple(Update.class, MapContainer.class, null, ""), //
						tuple(Insert.class, Element.class, "one", "elements") //
		);
	}

	@Test // DATAJDBC-130
	public void listTriggersDeletePlusInsert() {

		ListContainer entity = new ListContainer(SOME_ENTITY_ID);
		entity.elements.add(new Element(null));

		AggregateChange<ListContainer> aggregateChange = new AggregateChange(Kind.SAVE, ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, DbAction::getEntityType, this::getListKey, this::extractPath) //
				.containsExactly( //
						tuple(Delete.class, Element.class, null, "elements"), //
						tuple(Update.class, ListContainer.class, null, ""), //
						tuple(Insert.class, Element.class, 0, "elements") //
		);
	}

	private CascadingReferenceMiddleElement createMiddleElement(Element first, Element second) {

		CascadingReferenceMiddleElement middleElement1 = new CascadingReferenceMiddleElement(null);
		middleElement1.element.add(first);
		middleElement1.element.add(second);
		return middleElement1;
	}

	private Object getMapKey(DbAction a) {
		return a.getAdditionalValues().get("map_container_key");
	}

	private Object getListKey(DbAction a) {
		return a.getAdditionalValues().get("list_container_key");
	}

	private String extractPath(DbAction action) {
		return action.getPropertyPath().toDotPath();
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
	private static class MapContainer {

		@Id final Long id;
		Map<String, Element> elements = new HashMap<>();
	}

	@RequiredArgsConstructor
	private static class ListContainer {

		@Id final Long id;
		List<Element> elements = new ArrayList<>();
	}

	@RequiredArgsConstructor
	private static class Element {
		@Id final Long id;
	}

}
