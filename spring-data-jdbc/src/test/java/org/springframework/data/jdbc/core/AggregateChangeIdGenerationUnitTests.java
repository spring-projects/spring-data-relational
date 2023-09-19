/*
 * Copyright 2018-2023 the original author or authors.
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

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.conversion.RootAggregateChange;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Unit tests for the {@link MutableAggregateChange}.
 *
 * @author Jens Schauder
 * @author Myeonghyeon-Lee
 * @author Chirag Tailor
 */
public class AggregateChangeIdGenerationUnitTests {

	DummyEntity entity = new DummyEntity();
	Content content = new Content();
	Content content2 = new Content();
	Tag tag1 = new Tag();
	Tag tag2 = new Tag();
	Tag tag3 = new Tag();

	RelationalMappingContext context = new RelationalMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	DbAction.WithRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(entity, IdValueSource.GENERATED);
	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class, new IncrementingIds());
	AggregateChangeExecutor executor = new AggregateChangeExecutor(converter, accessStrategy);

	@Test // DATAJDBC-291
	public void singleRoot() {

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);

		executor.executeSave(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
	}

	@Test // DATAJDBC-291
	public void simpleReference() {

		entity.single = content;

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("single", content, null));

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.single.id).isEqualTo(2);
		});
	}

	@Test // DATAJDBC-291
	public void listReference() {

		entity.contentList.add(content);
		entity.contentList.add(content2);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("contentList", content, 0));
		aggregateChange.addAction(createInsert("contentList", content2, 1));

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList).extracting(c -> c.id).containsExactly(2, 3);
		});
	}

	@Test // DATAJDBC-291
	public void mapReference() {

		entity.contentMap.put("a", content);
		entity.contentMap.put("b", content2);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("contentMap", content, "a"));
		aggregateChange.addAction(createInsert("contentMap", content2, "b"));

		executor.executeSave(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.contentMap.values()).extracting(c -> c.id).containsExactly(2, 3);
	}

	@Test // DATAJDBC-291
	public void setIdForDeepReference() {

		content.single = tag1;
		entity.single = content;

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert = createDeepInsert("single", tag1, null, parentInsert);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert);

		executor.executeSave(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.single.id).isEqualTo(2);
		assertThat(entity.single.single.id).isEqualTo(3);
	}

	@Test // DATAJDBC-291
	public void setIdForDeepReferenceElementList() {

		content.tagList.add(tag1);
		content.tagList.add(tag2);
		entity.single = content;

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert1 = createDeepInsert("tagList", tag1, 0, parentInsert);
		DbAction.Insert<?> insert2 = createDeepInsert("tagList", tag2, 1, parentInsert);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.single.id).isEqualTo(2);
			softly.assertThat(entity.single.tagList).extracting(t -> t.id).containsExactly(3, 4);
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementSetElementSet() {

		content.tagSet.add(tag1);
		content.tagSet.add(tag2);
		entity.contentSet.add(content);

		DbAction.Insert<?> parentInsert = createInsert("contentSet", content, null);
		DbAction.Insert<?> insert1 = createDeepInsert("tagSet", tag1, null, parentInsert);
		DbAction.Insert<?> insert2 = createDeepInsert("tagSet", tag2, null, parentInsert);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentSet) //
					.extracting(c -> content.id) //
					.containsExactly(2); //
			softly.assertThat(entity.contentSet.stream() //
					.flatMap(c -> c.tagSet.stream())) //
					.extracting(t -> t.id) //
					.containsExactlyInAnyOrder(3, 4); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementListSingleReference() {

		content.single = tag1;
		content2.single = tag2;
		entity.contentList.add(content);
		entity.contentList.add(content2);

		DbAction.Insert<?> parentInsert1 = createInsert("contentList", content, 0);
		DbAction.Insert<?> parentInsert2 = createInsert("contentList", content2, 1);
		DbAction.Insert<?> insert1 = createDeepInsert("single", tag1, null, parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("single", tag2, null, parentInsert2);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList) //
					.extracting(c -> c.id, c -> c.single.id) //
					.containsExactly(tuple(2, 4), tuple(3, 5)); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementListElementList() {

		content.tagList.add(tag1);
		content2.tagList.add(tag2);
		content2.tagList.add(tag3);
		entity.contentList.add(content);
		entity.contentList.add(content2);

		DbAction.Insert<?> parentInsert1 = createInsert("contentList", content, 0);
		DbAction.Insert<?> parentInsert2 = createInsert("contentList", content2, 1);
		DbAction.Insert<?> insert1 = createDeepInsert("tagList", tag1, 0, parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("tagList", tag2, 0, parentInsert2);
		DbAction.Insert<?> insert3 = createDeepInsert("tagList", tag3, 1, parentInsert2);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList) //
					.extracting(c -> c.id) //
					.containsExactly(2, 3); //
			softly.assertThat(entity.contentList.stream() //
					.flatMap(c -> c.tagList.stream()) //
			).extracting(t -> t.id) //
					.containsExactly(4, 5, 6); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementMapElementMap() {

		content.tagMap.put("111", tag1);
		content2.tagMap.put("222", tag2);
		content2.tagMap.put("333", tag3);
		entity.contentMap.put("one", content);
		entity.contentMap.put("two", content2);

		DbAction.Insert<?> parentInsert1 = createInsert("contentMap", content, "one");
		DbAction.Insert<?> parentInsert2 = createInsert("contentMap", content2, "two");
		DbAction.Insert<?> insert1 = createDeepInsert("tagMap", tag1, "111", parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("tagMap", tag2, "222", parentInsert2);
		DbAction.Insert<?> insert3 = createDeepInsert("tagMap", tag3, "333", parentInsert2);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		executor.executeSave(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentMap.entrySet()) //
					.extracting(Map.Entry::getKey, e -> e.getValue().id) //
					.containsExactly(tuple("one", 2), tuple("two", 3)); //
			softly.assertThat(entity.contentMap.values().stream() //
					.flatMap(c -> c.tagMap.entrySet().stream())) //
					.extracting(Map.Entry::getKey, e -> e.getValue().id) //
					.containsExactly( //
							tuple("111", 4), //
							tuple("222", 5), //
							tuple("333", 6) //
			); //
		});
	}

	DbAction.Insert<?> createInsert(String propertyName, Object value, @Nullable Object key) {

		return new DbAction.Insert<>(value, context.getPersistentPropertyPath(propertyName, DummyEntity.class), rootInsert,
				key == null ? emptyMap() : singletonMap(toPath(propertyName), key), IdValueSource.GENERATED);
	}

	DbAction.Insert<?> createDeepInsert(String propertyName, Object value, @Nullable Object key,
			DbAction.Insert<?> parentInsert) {

		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = toPath(
				parentInsert.getPropertyPath().toDotPath() + "." + propertyName);

		return new DbAction.Insert<>(value, propertyPath, parentInsert,
				key == null ? emptyMap() : singletonMap(propertyPath, key), IdValueSource.GENERATED);
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> path.equals(p.toDotPath())).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	private static class DummyEntity {

		@Id Integer rootId;

		Content single;

		Set<Content> contentSet = new HashSet<>();

		List<Content> contentList = new ArrayList<>();

		Map<String, Content> contentMap = new HashMap<>();
	}

	private static class Content {

		@Id Integer id;

		Tag single;

		Set<Tag> tagSet = new HashSet<>();

		List<Tag> tagList = new ArrayList<>();

		Map<String, Tag> tagMap = new HashMap<>();
	}

	private static class Tag {
		@Id Integer id;
	}

	private static class IncrementingIds implements Answer<Object> {
		long id = 1;

		@Override
		public Object answer(InvocationOnMock invocation) {

			if (!invocation.getMethod().getReturnType().equals(Object.class)) {
				throw new UnsupportedOperationException("This mock does not support this invocation: " + invocation);
			}

			return id++;
		}
	}
}
