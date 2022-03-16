/*
 * Copyright 2017-2022 the original author or authors.
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

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertBatch;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.conversion.DbAction.UpdateRoot;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Unit tests for the {@link RelationalEntityWriter}
 *
 * @author Jens Schauder
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
@ExtendWith(MockitoExtension.class)
public class RelationalEntityWriterUnitTests {

	static final long SOME_ENTITY_ID = 23L;
	final RelationalMappingContext context = new RelationalMappingContext();
	final RelationalEntityWriter converter = new RelationalEntityWriter(context);

	final PersistentPropertyPath<RelationalPersistentProperty> listContainerElements = toPath("elements",
			ListContainer.class, context);

	private final PersistentPropertyPath<RelationalPersistentProperty> mapContainerElements = toPath("elements",
			MapContainer.class, context);

	private final PersistentPropertyPath<RelationalPersistentProperty> listMapContainerElements = toPath("maps.elements",
			ListMapContainer.class, context);

	private final PersistentPropertyPath<RelationalPersistentProperty> listMapContainerMaps = toPath("maps",
			ListMapContainer.class, context);

	private final PersistentPropertyPath<RelationalPersistentProperty> noIdListMapContainerElements = toPath(
			"maps.elements", NoIdListMapContainer.class, context);

	private final PersistentPropertyPath<RelationalPersistentProperty> noIdListMapContainerMaps = toPath("maps",
			NoIdListMapContainer.class, context);

	@Test // DATAJDBC-112
	public void newEntityGetsConvertedToOneInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		MutableAggregateChange<SingleReferenceEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false,
								IdValueSource.GENERATED) //
				);
	}

	@Test
	void newEntityWithPrimitiveLongId_insertDoesNotIncludeId_whenIdValueIsZero() {
		PrimitiveLongIdEntity entity = new PrimitiveLongIdEntity();

		MutableAggregateChange<PrimitiveLongIdEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, PrimitiveLongIdEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, PrimitiveLongIdEntity.class, "", PrimitiveLongIdEntity.class, false,
								IdValueSource.GENERATED) //
				);
	}

	@Test
	void newEntityWithPrimitiveIntId_insertDoesNotIncludeId_whenIdValueIsZero() {
		PrimitiveIntIdEntity entity = new PrimitiveIntIdEntity();

		MutableAggregateChange<PrimitiveIntIdEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, PrimitiveIntIdEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, PrimitiveIntIdEntity.class, "", PrimitiveIntIdEntity.class, false,
								IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-111
	public void newEntityGetsConvertedToOneInsertByEmbeddedEntities() {

		EmbeddedReferenceEntity entity = new EmbeddedReferenceEntity(null);
		entity.other = new Element(2L);

		MutableAggregateChange<EmbeddedReferenceEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, EmbeddedReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, EmbeddedReferenceEntity.class, "", EmbeddedReferenceEntity.class, false,
								IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-112
	public void newEntityWithReferenceGetsConvertedToTwoInserts() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		entity.other = new Element(null);

		MutableAggregateChange<SingleReferenceEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false,
								IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other", Element.class, true, IdValueSource.GENERATED) //
				);
	}

	@Test
	void newEntityWithReference_whenReferenceHasPrimitiveId_insertDoesNotIncludeId_whenIdValueIsZero() {

		EntityWithReferencesToPrimitiveIdEntity entity = new EntityWithReferencesToPrimitiveIdEntity(null);
		entity.primitiveLongIdEntity = new PrimitiveLongIdEntity();
		entity.primitiveIntIdEntity = new PrimitiveIntIdEntity();

		MutableAggregateChange<EntityWithReferencesToPrimitiveIdEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, EntityWithReferencesToPrimitiveIdEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(InsertRoot.class, EntityWithReferencesToPrimitiveIdEntity.class, "",
								EntityWithReferencesToPrimitiveIdEntity.class, false, IdValueSource.GENERATED), //
						tuple(Insert.class, PrimitiveLongIdEntity.class, "primitiveLongIdEntity", PrimitiveLongIdEntity.class, true,
								IdValueSource.GENERATED), //
						tuple(Insert.class, PrimitiveIntIdEntity.class, "primitiveIntIdEntity", PrimitiveIntIdEntity.class, true,
								IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToDeletePlusUpdate() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);

		MutableAggregateChange<SingleReferenceEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn) //
				.containsExactly( //
						tuple(UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false), //
						tuple(Delete.class, Element.class, "other", null, false) //
				);
	}

	@Test // DATAJDBC-112
	public void newReferenceTriggersDeletePlusInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);
		entity.other = new Element(null);

		MutableAggregateChange<SingleReferenceEntity> aggregateChange = new DefaultAggregateChange<>(
				AggregateChange.Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false, null), //
						tuple(Delete.class, Element.class, "other", null, false, null), //
						tuple(Insert.class, Element.class, "other", Element.class, true, IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-113
	public void newEntityWithEmptySetResultsInSingleInsert() {

		SetContainer entity = new SetContainer(null);
		MutableAggregateChange<SetContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				SetContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, SetContainer.class, "", SetContainer.class, false, IdValueSource.GENERATED));
	}

	@Test // DATAJDBC-113
	public void newEntityWithSetContainingMultipleElementsResultsInAnInsertForTheBatch() {

		SetContainer entity = new SetContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		MutableAggregateChange<SetContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				SetContainer.class, entity);
		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, SetContainer.class, "", SetContainer.class, false, IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, "", null, false, IdValueSource.GENERATED) //
				);
		List<Insert<Element>> batchedInsertActions = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(batchedInsertActions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, "elements", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "elements", Element.class, true, IdValueSource.GENERATED) //
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

		MutableAggregateChange<CascadingReferenceEntity> aggregateChange = new DefaultAggregateChange<>(
				AggregateChange.Kind.SAVE, CascadingReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, CascadingReferenceEntity.class, "", CascadingReferenceEntity.class, false,
								IdValueSource.GENERATED), //
						tuple(InsertBatch.class, CascadingReferenceMiddleElement.class, "", null, false, IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, "", null, false, IdValueSource.GENERATED) //
				);
		List<Insert<CascadingReferenceMiddleElement>> middleElementInserts = getInsertBatchAction(actions,
				CascadingReferenceMiddleElement.class).getInserts();
		assertThat(middleElementInserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED) //
				);
		List<Insert<Element>> leafElementInserts = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(leafElementInserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED) //
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

		MutableAggregateChange<CascadingReferenceEntity> aggregateChange = new DefaultAggregateChange<>(
				AggregateChange.Kind.SAVE, CascadingReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, CascadingReferenceEntity.class, "", CascadingReferenceEntity.class, false, null), //
						tuple(Delete.class, Element.class, "other.element", null, false, null),
						tuple(Delete.class, CascadingReferenceMiddleElement.class, "other", null, false, null),
						tuple(InsertBatch.class, CascadingReferenceMiddleElement.class, "", null, false, IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, "", null, false, IdValueSource.GENERATED) //
				);
		List<Insert<CascadingReferenceMiddleElement>> middleElementInserts = getInsertBatchAction(actions,
				CascadingReferenceMiddleElement.class).getInserts();
		assertThat(middleElementInserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED) //
				);
		List<Insert<Element>> elementInserts = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(elementInserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-131
	public void newEntityWithEmptyMapResultsInSingleInsert() {

		MapContainer entity = new MapContainer(null);
		MutableAggregateChange<MapContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, MapContainer.class, "", IdValueSource.GENERATED));
	}

	@Test // DATAJDBC-131
	public void newEntityWithMapResultsInAdditionalInsertPerElement() {

		MapContainer entity = new MapContainer(null);
		entity.elements.put("one", new Element(null));
		entity.elements.put("two", new Element(null));

		MutableAggregateChange<MapContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, null, "", IdValueSource.GENERATED) //
				);
		List<Insert<Element>> inserts = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(inserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(Insert.class, Element.class, "one", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "two", "elements", IdValueSource.GENERATED) //
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

		MutableAggregateChange<MapContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, null, "", IdValueSource.GENERATED) //
				);
		List<Insert<Element>> inserts = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(inserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(Insert.class, Element.class, "1", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "2", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "3", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "4", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "5", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "6", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "7", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "8", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "9", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "0", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "a", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "b", "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-130
	public void newEntityWithEmptyListResultsInSingleInsert() {

		ListContainer entity = new ListContainer(null);
		MutableAggregateChange<ListContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, ListContainer.class, "", IdValueSource.GENERATED));
	}

	@Test // DATAJDBC-130
	public void newEntityWithListResultsInAdditionalInsertPerElement() {

		ListContainer entity = new ListContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		MutableAggregateChange<ListContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				ListContainer.class, entity);
		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getListKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, ListContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, null, "", IdValueSource.GENERATED) //
				);
		List<Insert<Element>> inserts = getInsertBatchAction(actions, Element.class).getInserts();
		assertThat(inserts).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getListKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, 0, "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, 1, "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-131
	public void mapTriggersDeletePlusInsert() {

		MapContainer entity = new MapContainer(SOME_ENTITY_ID);
		entity.elements.put("one", new Element(null));

		MutableAggregateChange<MapContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						this::getMapKey, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, MapContainer.class, null, "", null), //
						tuple(Delete.class, Element.class, null, "elements", null), //
						tuple(Insert.class, Element.class, "one", "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-130
	public void listTriggersDeletePlusInsert() {

		ListContainer entity = new ListContainer(SOME_ENTITY_ID);
		entity.elements.add(new Element(null));

		MutableAggregateChange<ListContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						this::getListKey, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, ListContainer.class, null, "", null), //
						tuple(Delete.class, Element.class, null, "elements", null), //
						tuple(Insert.class, Element.class, 0, "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-223
	public void multiLevelQualifiedReferencesWithId() {

		ListMapContainer listMapContainer = new ListMapContainer(SOME_ENTITY_ID);
		listMapContainer.maps.add(new MapContainer(SOME_ENTITY_ID));
		listMapContainer.maps.get(0).elements.put("one", new Element(null));

		MutableAggregateChange<ListMapContainer> aggregateChange = new DefaultAggregateChange<>(AggregateChange.Kind.SAVE,
				ListMapContainer.class, listMapContainer);

		converter.write(listMapContainer, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						a -> getQualifier(a, listMapContainerMaps), //
						a -> getQualifier(a, listMapContainerElements), //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, ListMapContainer.class, null, null, "", null), //
						tuple(Delete.class, Element.class, null, null, "maps.elements", null), //
						tuple(Delete.class, MapContainer.class, null, null, "maps", null), //
						tuple(Insert.class, MapContainer.class, 0, null, "maps", IdValueSource.PROVIDED), //
						tuple(Insert.class, Element.class, null, "one", "maps.elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-223
	public void multiLevelQualifiedReferencesWithOutId() {

		NoIdListMapContainer listMapContainer = new NoIdListMapContainer(SOME_ENTITY_ID);
		listMapContainer.maps.add(new NoIdMapContainer());
		listMapContainer.maps.get(0).elements.put("one", new NoIdElement());

		MutableAggregateChange<NoIdListMapContainer> aggregateChange = new DefaultAggregateChange<>(
				AggregateChange.Kind.SAVE, NoIdListMapContainer.class, listMapContainer);

		converter.write(listMapContainer, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						a -> getQualifier(a, noIdListMapContainerMaps), //
						a -> getQualifier(a, noIdListMapContainerElements), //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, NoIdListMapContainer.class, null, null, "", null), //
						tuple(Delete.class, NoIdElement.class, null, null, "maps.elements", null), //
						tuple(Delete.class, NoIdMapContainer.class, null, null, "maps", null), //
						tuple(Insert.class, NoIdMapContainer.class, 0, null, "maps", IdValueSource.NONE), //
						tuple(Insert.class, NoIdElement.class, 0, "one", "maps.elements", IdValueSource.NONE) //
				);
	}

	@Test // DATAJDBC-417
	public void savingANullEmbeddedWithEntity() {

		EmbeddedReferenceChainEntity entity = new EmbeddedReferenceChainEntity(null);
		// the embedded is null !!!

		MutableAggregateChange<EmbeddedReferenceChainEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, EmbeddedReferenceChainEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, EmbeddedReferenceChainEntity.class, "", EmbeddedReferenceChainEntity.class, false,
								IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-417
	public void savingInnerNullEmbeddedWithEntity() {

		RootWithEmbeddedReferenceChainEntity root = new RootWithEmbeddedReferenceChainEntity(null);
		root.other = new EmbeddedReferenceChainEntity(null);
		// the embedded is null !!!

		MutableAggregateChange<RootWithEmbeddedReferenceChainEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, RootWithEmbeddedReferenceChainEntity.class, root);

		converter.write(root, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, RootWithEmbeddedReferenceChainEntity.class, "",
								RootWithEmbeddedReferenceChainEntity.class, false, IdValueSource.GENERATED), //
						tuple(Insert.class, EmbeddedReferenceChainEntity.class, "other", EmbeddedReferenceChainEntity.class, true,
								IdValueSource.GENERATED) //
				);
	}

	@Test
	void newEntityWithCollectionWhereSomeElementsHaveIdSet_producesABatchInsertEachForElementsWithIdAndWithout() {

		ListContainer root = new ListContainer(null);
		root.elements.add(new Element(null));
		root.elements.add(new Element(1L));
		root.elements.add(new Element(null));
		root.elements.add(new Element(2L));
		MutableAggregateChange<ListContainer> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, ListContainer.class, root);

		converter.write(root, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsSubsequence(
						tuple(InsertRoot.class, ListContainer.class, "", ListContainer.class, false, IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, "", null, false, IdValueSource.PROVIDED) //
				).containsSubsequence( //
						tuple(InsertRoot.class, ListContainer.class, "", ListContainer.class, false, IdValueSource.GENERATED), //
						tuple(InsertBatch.class, Element.class, "", null, false, IdValueSource.GENERATED) //
				);
		InsertBatch<Element> insertBatchWithoutId = getInsertBatchAction(actions, Element.class, IdValueSource.GENERATED);
		assertThat(insertBatchWithoutId.getInserts()).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getListKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, 0, "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, 2, "elements", IdValueSource.GENERATED) //
				);
		InsertBatch<Element> insertBatchWithId = getInsertBatchAction(actions, Element.class, IdValueSource.PROVIDED);
		assertThat(insertBatchWithId.getInserts()).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getListKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(Insert.class, Element.class, 1, "elements", IdValueSource.PROVIDED), //
						tuple(Insert.class, Element.class, 3, "elements", IdValueSource.PROVIDED) //
				);
	}

	@Test
	void newEntityWithCollection_whenElementHasPrimitiveId_batchInsertDoesNotIncludeId_whenIdValueIsZero() {

		EntityWithReferencesToPrimitiveIdEntity entity = new EntityWithReferencesToPrimitiveIdEntity(null);
		entity.primitiveLongIdEntities.add(new PrimitiveLongIdEntity());
		entity.primitiveIntIdEntities.add(new PrimitiveIntIdEntity());

		MutableAggregateChange<EntityWithReferencesToPrimitiveIdEntity> aggregateChange = //
				new DefaultAggregateChange<>(AggregateChange.Kind.SAVE, EntityWithReferencesToPrimitiveIdEntity.class, entity);

		converter.write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(InsertRoot.class, EntityWithReferencesToPrimitiveIdEntity.class, "",
								EntityWithReferencesToPrimitiveIdEntity.class, false, IdValueSource.GENERATED), //
						tuple(Insert.class, PrimitiveLongIdEntity.class, "primitiveLongIdEntities", PrimitiveLongIdEntity.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, PrimitiveIntIdEntity.class, "primitiveIntIdEntities", PrimitiveIntIdEntity.class, true,
								IdValueSource.GENERATED) //
				);
	}

	private List<DbAction<?>> extractActions(MutableAggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();
		aggregateChange.forEachAction(actions::add);
		return actions;
	}

	@NotNull
	private <T> InsertBatch<T> getInsertBatchAction(List<DbAction<?>> actions, Class<T> entityType) {
		return getInsertBatchActions(actions, entityType).stream().findFirst()
				.orElseThrow(() -> new RuntimeException("No InsertBatch action found!"));
	}

	@NotNull
	private <T> InsertBatch<T> getInsertBatchAction(List<DbAction<?>> actions, Class<T> entityType,
			IdValueSource idValueSource) {
		return getInsertBatchActions(actions, entityType).stream()
				.filter(insertBatch -> insertBatch.getIdValueSource() == idValueSource).findFirst().orElseThrow(
						() -> new RuntimeException(String.format("No InsertBatch with includeId '%s' found!", idValueSource)));
	}

	@NotNull
	private <T> List<InsertBatch<T>> getInsertBatchActions(List<DbAction<?>> actions, Class<T> entityType) {
		// noinspection unchecked
		return actions.stream() //
				.filter(dbAction -> dbAction instanceof InsertBatch) //
				.filter(dbAction -> dbAction.getEntityType().equals(entityType)) //
				.map(dbAction -> (InsertBatch<T>) dbAction).collect(Collectors.toList());
	}

	private CascadingReferenceMiddleElement createMiddleElement(Element first, Element second) {

		CascadingReferenceMiddleElement middleElement1 = new CascadingReferenceMiddleElement(null);
		middleElement1.element.add(first);
		middleElement1.element.add(second);
		return middleElement1;
	}

	private Object getMapKey(DbAction a) {

		PersistentPropertyPath<RelationalPersistentProperty> path = this.mapContainerElements;

		return getQualifier(a, path);
	}

	private Object getListKey(DbAction a) {

		PersistentPropertyPath<RelationalPersistentProperty> path = this.listContainerElements;

		return getQualifier(a, path);
	}

	@Nullable
	private Object getQualifier(DbAction a, PersistentPropertyPath<RelationalPersistentProperty> path) {

		return a instanceof DbAction.WithDependingOn //
				? ((DbAction.WithDependingOn) a).getQualifiers().get(path) //
				: null;
	}

	static PersistentPropertyPath<RelationalPersistentProperty> toPath(String path, Class source,
			RelationalMappingContext context) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(source, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst().orElse(null);
	}

	@RequiredArgsConstructor
	@Data
	static class EntityWithReferencesToPrimitiveIdEntity {
		@Id final Long id;
		PrimitiveLongIdEntity primitiveLongIdEntity;
		List<PrimitiveLongIdEntity> primitiveLongIdEntities = new ArrayList<>();
		PrimitiveIntIdEntity primitiveIntIdEntity;
		List<PrimitiveIntIdEntity> primitiveIntIdEntities = new ArrayList<>();
	}

	@Data
	static class PrimitiveLongIdEntity {
		@Id long id;
	}

	@Data
	static class PrimitiveIntIdEntity {
		@Id int id;
	}

	@RequiredArgsConstructor
	static class SingleReferenceEntity {

		@Id final Long id;
		Element other;
		// should not trigger own DbAction
		String name;
	}

	@RequiredArgsConstructor
	static class EmbeddedReferenceEntity {

		@Id final Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") Element other;
	}

	@RequiredArgsConstructor
	static class EmbeddedReferenceChainEntity {

		@Id final Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") ElementReference other;
	}

	@RequiredArgsConstructor
	static class RootWithEmbeddedReferenceChainEntity {

		@Id final Long id;
		EmbeddedReferenceChainEntity other;
	}

	@RequiredArgsConstructor
	static class ReferenceWoIdEntity {

		@Id final Long id;
		NoIdElement other;
		// should not trigger own DbAction
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
	private static class ListMapContainer {

		@Id final Long id;
		List<MapContainer> maps = new ArrayList<>();
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

	@RequiredArgsConstructor
	private static class ElementReference {
		final Element element;
	}

	@RequiredArgsConstructor
	private static class NoIdListMapContainer {

		@Id final Long id;
		List<NoIdMapContainer> maps = new ArrayList<>();
	}

	@RequiredArgsConstructor
	private static class NoIdMapContainer {

		Map<String, NoIdElement> elements = new HashMap<>();
	}

	@RequiredArgsConstructor
	private static class NoIdElement {
		// empty classes feel weird.
		String name;
	}
}
