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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.conversion.DbAction.UpdateRoot;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

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
		RootAggregateChange<SingleReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<SingleReferenceEntity>(context).write(entity, aggregateChange);

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

	@Test // GH-1159
	void newEntityWithPrimitiveLongId_insertDoesNotIncludeId_whenIdValueIsZero() {
		PrimitiveLongIdEntity entity = new PrimitiveLongIdEntity();

		RootAggregateChange<PrimitiveLongIdEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<PrimitiveLongIdEntity>(context).write(entity, aggregateChange);

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

	@Test // GH-1159
	void newEntityWithPrimitiveIntId_insertDoesNotIncludeId_whenIdValueIsZero() {
		PrimitiveIntIdEntity entity = new PrimitiveIntIdEntity();

		RootAggregateChange<PrimitiveIntIdEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<PrimitiveIntIdEntity>(context).write(entity, aggregateChange);

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

		RootAggregateChange<EmbeddedReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<EmbeddedReferenceEntity>(context).write(entity, aggregateChange);

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

		RootAggregateChange<SingleReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<SingleReferenceEntity>(context).write(entity, aggregateChange);

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

	@Test // GH-1159
	void newEntityWithReference_whenReferenceHasPrimitiveId_insertDoesNotIncludeId_whenIdValueIsZero() {

		EntityWithReferencesToPrimitiveIdEntity entity = new EntityWithReferencesToPrimitiveIdEntity(null);
		entity.primitiveLongIdEntity = new PrimitiveLongIdEntity();
		entity.primitiveIntIdEntity = new PrimitiveIntIdEntity();

		RootAggregateChange<EntityWithReferencesToPrimitiveIdEntity> aggregateChange = MutableAggregateChange
				.forSave(entity);

		new RelationalEntityWriter<EntityWithReferencesToPrimitiveIdEntity>(context).write(entity, aggregateChange);

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

		RootAggregateChange<SingleReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity, 1L);

		new RelationalEntityWriter<SingleReferenceEntity>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						dbAction -> dbAction instanceof UpdateRoot ? ((UpdateRoot<?>) dbAction).getPreviousVersion() : null) //
				.containsExactly( //
						tuple(UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false, 1L), //
						tuple(Delete.class, Element.class, "other", null, false, null) //
				);
	}

	@Test // DATAJDBC-112
	public void newReferenceTriggersDeletePlusInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);
		entity.other = new Element(null);

		RootAggregateChange<SingleReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity, 1L);

		new RelationalEntityWriter<SingleReferenceEntity>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false, IdValueSource.PROVIDED), //
						tuple(Delete.class, Element.class, "other", null, false, null), //
						tuple(Insert.class, Element.class, "other", Element.class, true, IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-113
	public void newEntityWithEmptySetResultsInSingleInsert() {

		SetContainer entity = new SetContainer(null);
		RootAggregateChange<SetContainer> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<SetContainer>(context).write(entity, aggregateChange);

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
	public void newEntityWithSetContainingMultipleElementsResultsInAnInsertForEach() {

		SetContainer entity = new SetContainer(null);
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		RootAggregateChange<SetContainer> aggregateChange = MutableAggregateChange.forSave(entity);
		new RelationalEntityWriter<SetContainer>(context).write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, SetContainer.class, "", SetContainer.class, false, IdValueSource.GENERATED), //
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

		RootAggregateChange<CascadingReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<CascadingReferenceEntity>(context).write(entity, aggregateChange);

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
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
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

		RootAggregateChange<CascadingReferenceEntity> aggregateChange = MutableAggregateChange.forSave(entity, 1L);

		new RelationalEntityWriter<CascadingReferenceEntity>(context).write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::actualEntityType, //
				DbActionTestSupport::isWithDependsOn, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, CascadingReferenceEntity.class, "", CascadingReferenceEntity.class, false, IdValueSource.PROVIDED), //
						tuple(Delete.class, Element.class, "other.element", null, false, null),
						tuple(Delete.class, CascadingReferenceMiddleElement.class, "other", null, false, null),
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, CascadingReferenceMiddleElement.class, "other", CascadingReferenceMiddleElement.class,
								true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "other.element", Element.class, true, IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-131
	public void newEntityWithEmptyMapResultsInSingleInsert() {

		MapContainer entity = new MapContainer(null);
		RootAggregateChange<MapContainer> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<MapContainer>(context).write(entity, aggregateChange);

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

		RootAggregateChange<MapContainer> aggregateChange = MutableAggregateChange.forSave(entity);
		new RelationalEntityWriter<MapContainer>(context).write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "one", "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "two", "elements", IdValueSource.GENERATED) //
				).containsSubsequence( // container comes before the elements
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "two", "elements", IdValueSource.GENERATED) //
				).containsSubsequence( // container comes before the elements
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, "one", "elements", IdValueSource.GENERATED) //
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

		RootAggregateChange<MapContainer> aggregateChange = MutableAggregateChange.forSave(entity);
		new RelationalEntityWriter<MapContainer>(context).write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getMapKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactlyInAnyOrder( //
						tuple(InsertRoot.class, MapContainer.class, null, "", IdValueSource.GENERATED), //
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
		RootAggregateChange<ListContainer> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<ListContainer>(context).write(entity, aggregateChange);

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

		RootAggregateChange<ListContainer> aggregateChange = MutableAggregateChange.forSave(entity);
		new RelationalEntityWriter<ListContainer>(context).write(entity, aggregateChange);

		List<DbAction<?>> actions = extractActions(aggregateChange);
		assertThat(actions).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				this::getListKey, //
				DbActionTestSupport::extractPath, //
				DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(InsertRoot.class, ListContainer.class, null, "", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, 0, "elements", IdValueSource.GENERATED), //
						tuple(Insert.class, Element.class, 1, "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-131
	public void mapTriggersDeletePlusInsert() {

		MapContainer entity = new MapContainer(SOME_ENTITY_ID);
		entity.elements.put("one", new Element(null));

		RootAggregateChange<MapContainer> aggregateChange = MutableAggregateChange.forSave(entity, 1L);

		new RelationalEntityWriter<MapContainer>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						this::getMapKey, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, MapContainer.class, null, "", IdValueSource.PROVIDED), //
						tuple(Delete.class, Element.class, null, "elements", null), //
						tuple(Insert.class, Element.class, "one", "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-130
	public void listTriggersDeletePlusInsert() {

		ListContainer entity = new ListContainer(SOME_ENTITY_ID);
		entity.elements.add(new Element(null));

		RootAggregateChange<ListContainer> aggregateChange = MutableAggregateChange.forSave(entity, 1L);

		new RelationalEntityWriter<ListContainer>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						this::getListKey, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, ListContainer.class, null, "", IdValueSource.PROVIDED), //
						tuple(Delete.class, Element.class, null, "elements", null), //
						tuple(Insert.class, Element.class, 0, "elements", IdValueSource.GENERATED) //
				);
	}

	@Test // DATAJDBC-223
	public void multiLevelQualifiedReferencesWithId() {

		ListMapContainer listMapContainer = new ListMapContainer(SOME_ENTITY_ID);
		listMapContainer.maps.add(new MapContainer(SOME_ENTITY_ID));
		listMapContainer.maps.get(0).elements.put("one", new Element(null));

		RootAggregateChange<ListMapContainer> aggregateChange = MutableAggregateChange.forSave(listMapContainer, 1L);

		new RelationalEntityWriter<ListMapContainer>(context).write(listMapContainer, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						a -> getQualifier(a, listMapContainerMaps), //
						a -> getQualifier(a, listMapContainerElements), //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, ListMapContainer.class, null, null, "", IdValueSource.PROVIDED), //
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

		RootAggregateChange<NoIdListMapContainer> aggregateChange = MutableAggregateChange.forSave(listMapContainer,
				1L);

		new RelationalEntityWriter<NoIdListMapContainer>(context).write(listMapContainer, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						a -> getQualifier(a, noIdListMapContainerMaps), //
						a -> getQualifier(a, noIdListMapContainerElements), //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::insertIdValueSource) //
				.containsExactly( //
						tuple(UpdateRoot.class, NoIdListMapContainer.class, null, null, "", IdValueSource.PROVIDED), //
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

		RootAggregateChange<EmbeddedReferenceChainEntity> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<EmbeddedReferenceChainEntity>(context).write(entity, aggregateChange);

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

		RootAggregateChange<RootWithEmbeddedReferenceChainEntity> aggregateChange = MutableAggregateChange
				.forSave(root);

		new RelationalEntityWriter<RootWithEmbeddedReferenceChainEntity>(context).write(root, aggregateChange);

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

	@Test // GH-1159
	void newEntityWithCollection_whenElementHasPrimitiveId_doesNotIncludeId_whenIdValueIsZero() {

		EntityWithReferencesToPrimitiveIdEntity entity = new EntityWithReferencesToPrimitiveIdEntity(null);
		entity.primitiveLongIdEntities.add(new PrimitiveLongIdEntity());
		entity.primitiveIntIdEntities.add(new PrimitiveIntIdEntity());

		RootAggregateChange<EntityWithReferencesToPrimitiveIdEntity> aggregateChange = MutableAggregateChange
				.forSave(entity);

		new RelationalEntityWriter<EntityWithReferencesToPrimitiveIdEntity>(context).write(entity, aggregateChange);

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

	@Test // GH-1249
	public void readOnlyReferenceDoesNotCreateInsertsOnCreation() {

		WithReadOnlyReference entity = new WithReadOnlyReference(null);
		entity.readOnly = new Element(SOME_ENTITY_ID);

		RootAggregateChange<WithReadOnlyReference> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<WithReadOnlyReference>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath,
						DbActionTestSupport::actualEntityType, DbActionTestSupport::isWithDependsOn) //
				.containsExactly( //
						tuple(InsertRoot.class, WithReadOnlyReference.class, "", WithReadOnlyReference.class, false) //
						// no insert for element
				);

	}

	@Test // GH-1249
	public void readOnlyReferenceDoesNotCreateDeletesOrInsertsDuringUpdate() {

		WithReadOnlyReference entity = new WithReadOnlyReference(SOME_ENTITY_ID);
		entity.readOnly = new Element(SOME_ENTITY_ID);

		RootAggregateChange<WithReadOnlyReference> aggregateChange = MutableAggregateChange.forSave(entity);

		new RelationalEntityWriter<WithReadOnlyReference>(context).write(entity, aggregateChange);

		assertThat(extractActions(aggregateChange)) //
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::extractPath,
						DbActionTestSupport::actualEntityType, DbActionTestSupport::isWithDependsOn) //
				.containsExactly( //
						tuple(UpdateRoot.class, WithReadOnlyReference.class, "", WithReadOnlyReference.class, false) //
						// no insert for element
				);

	}

	private List<DbAction<?>> extractActions(MutableAggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();
		aggregateChange.forEachAction(actions::add);
		return actions;
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

	static class EntityWithReferencesToPrimitiveIdEntity {
		@Id final Long id;
		PrimitiveLongIdEntity primitiveLongIdEntity;
		List<PrimitiveLongIdEntity> primitiveLongIdEntities = new ArrayList<>();
		PrimitiveIntIdEntity primitiveIntIdEntity;
		List<PrimitiveIntIdEntity> primitiveIntIdEntities = new ArrayList<>();

		EntityWithReferencesToPrimitiveIdEntity(Long id) {
			this.id = id;
		}
	}

	static class PrimitiveLongIdEntity {
		@Id long id;
	}

	static class PrimitiveIntIdEntity {
		@Id int id;
	}

	static class SingleReferenceEntity {

		@Id
		final Long id;
		Element other;
		// should not trigger own DbAction
		String name;

		public SingleReferenceEntity(Long id) {
			this.id = id;
		}
	}

	static class EmbeddedReferenceEntity {

		@Id
		final Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_")
		Element other;

		public EmbeddedReferenceEntity(Long id) {
			this.id = id;
		}
	}

	static class EmbeddedReferenceChainEntity {

		@Id
		final Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_")
		ElementReference other;

		public EmbeddedReferenceChainEntity(Long id) {
			this.id = id;
		}
	}

	static class RootWithEmbeddedReferenceChainEntity {

		@Id
		final Long id;
		EmbeddedReferenceChainEntity other;

		public RootWithEmbeddedReferenceChainEntity(Long id) {
			this.id = id;
		}
	}

	static class ReferenceWoIdEntity {

		@Id
		final Long id;
		NoIdElement other;
		// should not trigger own DbAction
		String name;

		public ReferenceWoIdEntity(Long id) {
			this.id = id;
		}
	}

	private static class CascadingReferenceMiddleElement {

		@Id
		final Long id;
		final Set<Element> element = new HashSet<>();

		public CascadingReferenceMiddleElement(Long id) {
			this.id = id;
		}
	}

	private static class CascadingReferenceEntity {

		@Id
		final Long id;
		final Set<CascadingReferenceMiddleElement> other = new HashSet<>();

		public CascadingReferenceEntity(Long id) {
			this.id = id;
		}
	}

	private static class SetContainer {

		@Id
		final Long id;
		Set<Element> elements = new HashSet<>();

		public SetContainer(Long id) {
			this.id = id;
		}
	}

	private static class ListMapContainer {

		@Id
		final Long id;
		List<MapContainer> maps = new ArrayList<>();

		public ListMapContainer(Long id) {
			this.id = id;
		}
	}

	private static class MapContainer {

		@Id
		final Long id;
		Map<String, Element> elements = new HashMap<>();

		public MapContainer(Long id) {
			this.id = id;
		}
	}

	private static class ListContainer {

		@Id
		final Long id;
		List<Element> elements = new ArrayList<>();

		public ListContainer(Long id) {
			this.id = id;
		}
	}

	private static class Element {
		@Id
		final Long id;

		public Element(Long id) {
			this.id = id;
		}
	}

	private static class ElementReference {
		final Element element;

		public ElementReference(Element element) {
			this.element = element;
		}
	}

	private static class NoIdListMapContainer {

		@Id
		final Long id;
		List<NoIdMapContainer> maps = new ArrayList<>();

		public NoIdListMapContainer(Long id) {
			this.id = id;
		}
	}

	private static class NoIdMapContainer {

		Map<String, NoIdElement> elements = new HashMap<>();

		public NoIdMapContainer() {
		}
	}

	private static class NoIdElement {
		// empty classes feel weird.
		String name;

		public NoIdElement() {
		}
	}

	private static class WithReadOnlyReference {

		@Id
		final Long id;
		@ReadOnlyProperty
		Element readOnly;

		public WithReadOnlyReference(Long id) {
			this.id = id;
		}
	}

}
