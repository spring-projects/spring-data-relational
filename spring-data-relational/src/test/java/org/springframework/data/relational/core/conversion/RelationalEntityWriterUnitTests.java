/*
 * Copyright 2017-2019 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.With;

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
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.AggregateChange.Kind;
import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
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
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationalEntityWriterUnitTests {

	static final long SOME_ENTITY_ID = 23L;
	static final RelationalMappingContext context = new RelationalMappingContext();
	final RelationalEntityWriter converter = new RelationalEntityWriter(context);

	final PersistentPropertyPath<RelationalPersistentProperty> listContainerElements = toPath("elements",
			ListContainer.class);

	private final PersistentPropertyPath<RelationalPersistentProperty> mapContainerElements = toPath("elements",
			MapContainer.class);

	private final PersistentPropertyPath<RelationalPersistentProperty> listMapContainerElements = toPath("maps.elements",
			ListMapContainer.class);

	private final PersistentPropertyPath<RelationalPersistentProperty> listMapContainerMaps = toPath("maps",
			ListMapContainer.class);

	private final PersistentPropertyPath<RelationalPersistentProperty> noIdListMapContainerElements = toPath(
			"maps.elements", NoIdListMapContainer.class);

	private final PersistentPropertyPath<RelationalPersistentProperty> noIdListMapContainerMaps = toPath("maps",
			NoIdListMapContainer.class);

	@Test // DATAJDBC-112
	public void newEntityGetsConvertedToOneInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange<>(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter loggingInterpreter = new LoggingInterpreter();
		aggregateChange.executeWith(loggingInterpreter, context, new BasicRelationalConverter(context));

		assertThat(loggingInterpreter.log)
				.containsExactly(new LogEntry(InsertRoot.class, SingleReferenceEntity.class, "", false));
	}

	@Test // DATAJDBC-111
	public void newEntityGetsConvertedToOneInsertByEmbeddedEntities() {

		EmbeddedReferenceEntity entity = new EmbeddedReferenceEntity();
		entity.other = new Element(2L);

		AggregateChange<EmbeddedReferenceEntity> aggregateChange = //
				new AggregateChange<>(Kind.SAVE, EmbeddedReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log)
				.containsExactly(new LogEntry(InsertRoot.class, EmbeddedReferenceEntity.class, "", false));
	}

	@Test // DATAJDBC-112
	public void newEntityWithReferenceGetsConvertedToTwoInserts() {

		SingleReferenceEntity entity = new SingleReferenceEntity(null);
		entity.other = new Element(null);

		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange<>(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(InsertRoot.class, SingleReferenceEntity.class, "", false), //
				new LogEntry(Insert.class, Element.class, "other", true) //
		);
	}

	@Test // DATAJDBC-112
	public void existingEntityGetsConvertedToDeletePlusUpdate() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);

		AggregateChange<SingleReferenceEntity> aggregateChange = //
				new AggregateChange<>(Kind.SAVE, SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "other", false), //
				new LogEntry(UpdateRoot.class, SingleReferenceEntity.class, "", false) //
		);
	}

	@Test // DATAJDBC-112
	public void newReferenceTriggersDeletePlusInsert() {

		SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);
		entity.other = new Element(null);

		AggregateChange<SingleReferenceEntity> aggregateChange = new AggregateChange<>(Kind.SAVE,
				SingleReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()) //
				.extracting(DbAction::getClass, //
						DbAction::getEntityType, //
						DbActionTestSupport::extractPath, //
						DbActionTestSupport::actualEntityType, //
						DbActionTestSupport::isWithDependsOn) //
				.containsExactly( //
						tuple(Delete.class, Element.class, "other", null, false), //
						tuple(UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false), //
						tuple(Insert.class, Element.class, "other", Element.class, true) //
				);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "other", false), //
				new LogEntry(UpdateRoot.class, SingleReferenceEntity.class, "", false), //
				new LogEntry(Insert.class, Element.class, "other", true) //
		); //

	}

	@Test // DATAJDBC-113
	public void newEntityWithEmptySetResultsInSingleInsert() {

		SetContainer entity = new SetContainer();
		AggregateChange<RelationalEntityWriterUnitTests.SetContainer> aggregateChange = new AggregateChange<>(Kind.SAVE,
				SetContainer.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly(new LogEntry(InsertRoot.class, SetContainer.class, "", false));
	}

	@Test // DATAJDBC-113
	public void newEntityWithSetResultsInAdditionalInsertPerElement() {

		SetContainer entity = new SetContainer();
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		AggregateChange<SetContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, SetContainer.class, entity);
		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(InsertRoot.class, SetContainer.class, "", false), //
				new LogEntry(Insert.class, Element.class, "elements", true), //
				new LogEntry(Insert.class, Element.class, "elements", true) //
		);
	}

	@Test // DATAJDBC-113
	public void cascadingReferencesTriggerCascadingActions() {

		CascadingReferenceEntity entity = new CascadingReferenceEntity();

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		AggregateChange<CascadingReferenceEntity> aggregateChange = new AggregateChange<>(Kind.SAVE,
				CascadingReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(InsertRoot.class, CascadingReferenceEntity.class, "", false), //
				new LogEntry(Insert.class, CascadingReferenceMiddleElement.class, "other", true), //
				new LogEntry(Insert.class, CascadingReferenceMiddleElement.class, "other", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true) //
		);
	}

	@Test // DATAJDBC-188
	public void cascadingReferencesTriggerCascadingActionsForUpdate() {

		CascadingReferenceEntity entity = new CascadingReferenceEntity().withId(23L);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		entity.other.add(createMiddleElement( //
				new Element(null), //
				new Element(null)) //
		);

		AggregateChange<CascadingReferenceEntity> aggregateChange = new AggregateChange<>(Kind.SAVE,
				CascadingReferenceEntity.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "other.element", false), //
				new LogEntry(Delete.class, CascadingReferenceMiddleElement.class, "other", false), //
				new LogEntry(UpdateRoot.class, CascadingReferenceEntity.class, "", false), //
				new LogEntry(Insert.class, CascadingReferenceMiddleElement.class, "other", true), //
				new LogEntry(Insert.class, CascadingReferenceMiddleElement.class, "other", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true), //
				new LogEntry(Insert.class, Element.class, "other.element", true) //
		);
	}

	@Test // DATAJDBC-131
	public void newEntityWithEmptyMapResultsInSingleInsert() {

		MapContainer entity = new MapContainer();
		AggregateChange<MapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		assertThat(aggregateChange.getActions()).extracting(DbAction::getClass, //
				DbAction::getEntityType, //
				DbActionTestSupport::extractPath) //
				.containsExactly( //
						tuple(InsertRoot.class, MapContainer.class, ""));

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly(new LogEntry(InsertRoot.class, MapContainer.class, "", false));
	}

	@Test // DATAJDBC-131
	public void newEntityWithMapResultsInAdditionalInsertPerElement() {

		MapContainer entity = new MapContainer();
		entity.elements.put("one", new Element(null));
		entity.elements.put("two", new Element(null));

		AggregateChange<MapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(mapContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		LogEntry root = new LogEntry(InsertRoot.class, MapContainer.class, "", false, (Object) null);
		LogEntry one = new LogEntry(Insert.class, Element.class, "elements", true, "one");
		LogEntry two = new LogEntry(Insert.class, Element.class, "elements", true, "two");

		assertThat(interpreter.log).containsExactlyInAnyOrder(root, one, two).containsSubsequence(root, one)
				.containsSubsequence(root, two);
	}

	@Test // DATAJDBC-183
	public void newEntityWithFullMapResultsInAdditionalInsertPerElement() {

		MapContainer entity = new MapContainer();

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

		AggregateChange<MapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, MapContainer.class, entity);
		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(mapContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactlyInAnyOrder( //
				new LogEntry(InsertRoot.class, MapContainer.class, "", false, (Object) null), //
				new LogEntry(Insert.class, Element.class, "elements", true, "1"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "2"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "3"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "4"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "5"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "6"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "7"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "8"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "9"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "0"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "a"), //
				new LogEntry(Insert.class, Element.class, "elements", true, "b") //
		);

	}

	@Test // DATAJDBC-130
	public void newEntityWithEmptyListResultsInSingleInsert() {

		ListContainer entity = new ListContainer();
		AggregateChange<ListContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter();
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly(new LogEntry(InsertRoot.class, ListContainer.class, "", false));
	}

	@Test // DATAJDBC-130
	public void newEntityWithListResultsInAdditionalInsertPerElement() {

		ListContainer entity = new ListContainer();
		entity.elements.add(new Element(null));
		entity.elements.add(new Element(null));

		AggregateChange<ListContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, ListContainer.class, entity);
		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(listContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		LogEntry root = new LogEntry(InsertRoot.class, ListContainer.class, "", false, (Object) null);
		LogEntry zero = new LogEntry(Insert.class, Element.class, "elements", true, 0);
		LogEntry one = new LogEntry(Insert.class, Element.class, "elements", true, 1);

		assertThat(interpreter.log).containsExactlyInAnyOrder(root, zero, one).containsSubsequence(root, zero)
				.containsSubsequence(root, one);
	}

	@Test // DATAJDBC-131
	public void mapTriggersDeletePlusInsert() {

		MapContainer entity = new MapContainer().withId(SOME_ENTITY_ID);
		entity.elements.put("one", new Element(null));

		AggregateChange<MapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, MapContainer.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(mapContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "elements", false, (Object) null), //
				new LogEntry(UpdateRoot.class, MapContainer.class, "", false, (Object) null), //
				new LogEntry(Insert.class, Element.class, "elements", true, "one") //
		); //

	}

	@Test // DATAJDBC-130
	public void listTriggersDeletePlusInsert() {

		ListContainer entity = new ListContainer().withId(SOME_ENTITY_ID);
		entity.elements.add(new Element(null));

		AggregateChange<ListContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, ListContainer.class, entity);

		converter.write(entity, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(listContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "elements", false, (Object) null), //
				new LogEntry(UpdateRoot.class, ListContainer.class, "", false, (Object) null), //
				new LogEntry(Insert.class, Element.class, "elements", true, 0) //
		); //
	}

	@Test // DATAJDBC-223
	public void multiLevelQualifiedReferencesWithId() {

		ListMapContainer listMapContainer = new ListMapContainer(SOME_ENTITY_ID);
		listMapContainer.maps.add(new MapContainer().withId(SOME_ENTITY_ID));
		listMapContainer.maps.get(0).elements.put("one", new Element(null));

		AggregateChange<ListMapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, ListMapContainer.class,
				listMapContainer);

		converter.write(listMapContainer, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(listMapContainerMaps, listMapContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, Element.class, "maps.elements", false, null, null), //
				new LogEntry(Delete.class, MapContainer.class, "maps", false, null, null), //
				new LogEntry(UpdateRoot.class, ListMapContainer.class, "", false, null, null), //
				new LogEntry(Insert.class, MapContainer.class, "maps", true, 0, null), //
				new LogEntry(Insert.class, Element.class, "maps.elements", true, 0, "one") //
		); //
	}

	@Test // DATAJDBC-223
	public void multiLevelQualifiedReferencesWithOutId() {

		NoIdListMapContainer listMapContainer = new NoIdListMapContainer(SOME_ENTITY_ID);
		listMapContainer.maps.add(new NoIdMapContainer());
		listMapContainer.maps.get(0).elements.put("one", new NoIdElement());

		AggregateChange<NoIdListMapContainer> aggregateChange = new AggregateChange<>(Kind.SAVE, NoIdListMapContainer.class,
				listMapContainer);

		converter.write(listMapContainer, aggregateChange);

		LoggingInterpreter interpreter = new LoggingInterpreter(noIdListMapContainerMaps, noIdListMapContainerElements);
		aggregateChange.executeWith(interpreter, context, new BasicRelationalConverter(context));

		assertThat(interpreter.log).containsExactly( //
				new LogEntry(Delete.class, NoIdElement.class, "maps.elements", false, null, null), //
				new LogEntry(Delete.class, NoIdMapContainer.class, "maps", false, null, null), //
				new LogEntry(UpdateRoot.class, NoIdListMapContainer.class, "", false, null, null), //
				new LogEntry(Insert.class, NoIdMapContainer.class, "maps", true, 0, null), //
				new LogEntry(Insert.class, NoIdElement.class, "maps.elements", true, 0, "one") //
		); //
	}

	private CascadingReferenceMiddleElement createMiddleElement(Element first, Element second) {

		CascadingReferenceMiddleElement middleElement1 = new CascadingReferenceMiddleElement();
		middleElement1.element.add(first);
		middleElement1.element.add(second);
		return middleElement1;
	}

	@Nullable
	private static Object getQualifier(DbAction a, PersistentPropertyPath<RelationalPersistentProperty> path) {

		Object newValue = null;

		if (a instanceof DbAction.WithDependingOn) {

			DbAction.WithDependingOn withDependingOn = (DbAction.WithDependingOn) a;
			Map<String, Object> qualifiers = withDependingOn.getIdentifier(context, path).toMap();
			String owner = path.getRequiredLeafProperty().getKeyColumn();
			newValue = qualifiers.get(owner);
		}

		return newValue;
	}

	static PersistentPropertyPath<RelationalPersistentProperty> toPath(String path, Class source) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(source, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst().orElse(null);
	}

	@RequiredArgsConstructor
	@AllArgsConstructor
	static class SingleReferenceEntity {

		@With @Id final Long id;
		Element other;
		// should not trigger own DbAction
		String name;
	}

	@AllArgsConstructor
	@With
	static class EmbeddedReferenceEntity {

		@Id final Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") Element other;

		EmbeddedReferenceEntity() {
			id = null;
			other = null;
		}
	}

	@RequiredArgsConstructor
	static class ReferenceWoIdEntity {

		@Id final Long id;
		NoIdElement other;
		// should not trigger own DbAction
		String name;
	}

	@With
	@AllArgsConstructor
	static class CascadingReferenceMiddleElement {

		@Id final Long id;
		final Set<Element> element;

		CascadingReferenceMiddleElement() {
			id = null;
			element = new HashSet<>();
		}

	}

	@AllArgsConstructor
	@With
	private static class CascadingReferenceEntity {

		@Id final Long id;
		final Set<CascadingReferenceMiddleElement> other;

		CascadingReferenceEntity() {
			id = null;
			other = new HashSet<>();
		}
	}

	@AllArgsConstructor
	@With
	private static class SetContainer {

		@Id final Long id;
		Set<Element> elements;

		SetContainer() {
			id = null;
			elements = new HashSet<>();
		}
	}

	@RequiredArgsConstructor
	private static class ListMapContainer {

		@Id final Long id;
		List<MapContainer> maps = new ArrayList<>();
	}

	@AllArgsConstructor
	@With
	private static class MapContainer {

		@Id final Long id;
		Map<String, Element> elements = new HashMap<>();

		MapContainer() {
			id = null;
		}
	}

	@AllArgsConstructor
	@With
	private static class ListContainer {

		ListContainer() {
			id = null;
			elements = new ArrayList<>();
		}

		@Id final Long id;
		List<Element> elements;
	}

	@RequiredArgsConstructor
	private static class Element {
		@With @Id final Long id;
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
	@With
	@AllArgsConstructor
	private static class NoIdElement {
		// empty classes feel weird.
		String name;
	}

	private static class LoggingInterpreter implements Interpreter {

		private int id;
		private final PersistentPropertyPath<RelationalPersistentProperty>[] path;
		List<LogEntry> log = new ArrayList<>();

		@SafeVarargs
		public LoggingInterpreter(PersistentPropertyPath<RelationalPersistentProperty>... path) {
			this.path = path;
		}

		private <T> void process(DbAction<T> action) {
			Object[] qualifiers = new Object[path.length];
			for (int i = path.length - 1; i >= 0; i--) {
				qualifiers[i] = getQualifier(action, path[i]);
			}

			log.add(new LogEntry(action.getClass(), action.getEntityType(), DbActionTestSupport.extractPath(action),
					action instanceof DbAction.WithDependingOn, qualifiers));
		}

		@Override
		public <T> void interpret(Insert<T> insert) {

			process(insert);
			if (insert.getPropertyPath().getLeafProperty().getEntity().hasIdProperty()) {
				insert.setGeneratedId(++id);
			}
		}

		@Override
		public <T> void interpret(InsertRoot<T> insert) {
			process(insert);
			insert.setGeneratedId(++id);
		}

		@Override
		public <T> void interpret(DbAction.Update<T> update) {
			process(update);
		}

		@Override
		public <T> void interpret(UpdateRoot<T> update) {
			process(update);
		}

		@Override
		public <T> void interpret(DbAction.Merge<T> update) {
			process(update);
		}

		@Override
		public <T> void interpret(Delete<T> delete) {
			process(delete);
		}

		@Override
		public <T> void interpret(DbAction.DeleteRoot<T> deleteRoot) {
			process(deleteRoot);
		}

		@Override
		public <T> void interpret(DbAction.DeleteAll<T> delete) {
			process(delete);
		}

		@Override
		public <T> void interpret(DbAction.DeleteAllRoot<T> delete) {
			process(delete);
		}
	}

	@EqualsAndHashCode
	@ToString
	private static class LogEntry {
		private final Class<? extends DbAction> actionClass;
		private final Class<?> entityType;
		private final String path;
		private final boolean hasDependentOn;
		private final Object[] qualifiers;

		public LogEntry(Class<? extends DbAction> actionClass, Class<?> entityType, String path, boolean hasDependentOn,
				Object... qualifiers) {

			this.actionClass = actionClass;
			this.entityType = entityType;
			this.path = path;
			this.hasDependentOn = hasDependentOn;
			this.qualifiers = qualifiers;
		}
	}
}
