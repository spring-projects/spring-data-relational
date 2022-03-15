/*
 * Copyright 2018-2022 the original author or authors.
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

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.With;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Unit tests for the {@link MutableAggregateChange} testing the setting of generated ids in aggregates consisting of
 * immutable entities.
 *
 * @author Jens Schauder
 * @author Myeonghyeon-Lee
 * @author Chirag Tailor
 */
@Disabled
public class AggregateChangeIdGenerationImmutableUnitTests {

	DummyEntity entity = new DummyEntity();
	Content content = new Content();
	Content content2 = new Content();
	Tag tag1 = new Tag("tag1");
	Tag tag2 = new Tag("tag2");
	Tag tag3 = new Tag("tag3");
	ContentNoId contentNoId = new ContentNoId();
	ContentNoId contentNoId2 = new ContentNoId();

	RelationalMappingContext context = new RelationalMappingContext();
	JdbcConverter converter = mock(JdbcConverter.class);
	DbAction.WithEntity<?> rootInsert = new DbAction.InsertRoot<>(entity, IdValueSource.GENERATED);

	private DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

	AggregateChangeExecutor executor = new AggregateChangeExecutor(converter, accessStrategy);

	@Test // DATAJDBC-291
	public void singleRoot() {

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);

		entity = executor.execute(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
	}

	@Test // DATAJDBC-291
	public void simpleReference() {

		entity = entity.withSingle(content);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(createInsert("single", content, null));

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.single.id).isEqualTo(2);
		});
	}

	@Test // DATAJDBC-291
	public void listReference() {

		entity = entity.withContentList(asList(content, content2));

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(createInsert("contentList", content, 0));
		aggregateChange.addAction(createInsert("contentList", content2, 1));

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList).extracting(c -> c.id).containsExactly(2, 3);
		});
	}

	@Test // DATAJDBC-291
	public void mapReference() {

		entity = entity.withContentMap(createContentMap("a", content, "b", content2));

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(createInsert("contentMap", content, "a"));
		aggregateChange.addAction(createInsert("contentMap", content2, "b"));

		entity = executor.execute(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.contentMap.values()).extracting(c -> c.id).containsExactly(2, 3);
	}

	@Test // DATAJDBC-291
	public void setIdForDeepReference() {

		content = content.withSingle(tag1);
		entity = entity.withSingle(content);

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert = createDeepInsert("single", tag1, null, parentInsert);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert);

		entity = executor.execute(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.single.id).isEqualTo(2);
		assertThat(entity.single.single.id).isEqualTo(3);
	}

	@Test // DATAJDBC-291
	public void setIdForDeepReferenceElementList() {

		content = content.withTagList(asList(tag1, tag2));
		entity = entity.withSingle(content);

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert1 = createDeepInsert("tagList", tag1, 0, parentInsert);
		DbAction.Insert<?> insert2 = createDeepInsert("tagList", tag2, 1, parentInsert);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.single.id).isEqualTo(2);
			softly.assertThat(entity.single.tagList).extracting(t -> t.id).containsExactly(3, 4);
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementSetElementSet() {

		content = content.withTagSet(Stream.of(tag1, tag2).collect(Collectors.toSet()));
		entity = entity.withContentSet(singleton(content));

		DbAction.Insert<?> parentInsert = createInsert("contentSet", content, null);
		DbAction.Insert<?> insert1 = createDeepInsert("tagSet", tag1, null, parentInsert);
		DbAction.Insert<?> insert2 = createDeepInsert("tagSet", tag2, null, parentInsert);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentSet) //
					.extracting(c -> c.id) //
					.containsExactly(2); //
			softly.assertThat(entity.contentSet.stream() //
					.flatMap(c -> c.tagSet.stream())) //
					.extracting(t -> t.id) //
					.containsExactlyInAnyOrder(3, 4); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementListSingleReference() {

		content = content.withSingle(tag1);
		content2 = content2.withSingle(tag2);
		entity = entity.withContentList(asList(content, content2));

		DbAction.Insert<?> parentInsert1 = createInsert("contentList", content, 0);
		DbAction.Insert<?> parentInsert2 = createInsert("contentList", content2, 1);
		DbAction.Insert<?> insert1 = createDeepInsert("single", tag1, null, parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("single", tag2, null, parentInsert2);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList) //
					.extracting(c -> c.id, c -> c.single.id) //
					.containsExactly(tuple(2, 4), tuple(3, 5)); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForDeepElementListElementList() {

		content = content.withTagList(singletonList(tag1));
		content2 = content2.withTagList(asList(tag2, tag3));
		entity = entity.withContentList(asList(content, content2));

		DbAction.Insert<?> parentInsert1 = createInsert("contentList", content, 0);
		DbAction.Insert<?> parentInsert2 = createInsert("contentList", content2, 1);
		DbAction.Insert<?> insert1 = createDeepInsert("tagList", tag1, 0, parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("tagList", tag2, 0, parentInsert2);
		DbAction.Insert<?> insert3 = createDeepInsert("tagList", tag3, 1, parentInsert2);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		entity = executor.execute(aggregateChange);

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

		content = content.withTagMap(createTagMap("111", tag1, "222", tag2, "333", tag3));
		entity = entity.withContentMap(createContentMap("one", content, "two", content2));

		DbAction.Insert<?> parentInsert1 = createInsert("contentMap", content, "one");
		DbAction.Insert<?> parentInsert2 = createInsert("contentMap", content2, "two");
		DbAction.Insert<?> insert1 = createDeepInsert("tagMap", tag1, "111", parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("tagMap", tag2, "222", parentInsert2);
		DbAction.Insert<?> insert3 = createDeepInsert("tagMap", tag3, "333", parentInsert2);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		entity = executor.execute(aggregateChange);

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

	@Test // DATAJDBC-291
	public void setIdForDeepElementListSingleReferenceWithIntermittentNoId() {

		contentNoId = contentNoId.withSingle(tag1);
		contentNoId2 = contentNoId2.withSingle(tag2);
		entity = entity.withContentNoIdList(asList(contentNoId, contentNoId2));

		DbAction.Insert<?> parentInsert1 = createInsert("contentNoIdList", contentNoId, 0);
		DbAction.Insert<?> parentInsert2 = createInsert("contentNoIdList", contentNoId2, 1);
		DbAction.Insert<?> insert1 = createDeepInsert("single", tag1, null, parentInsert1);
		DbAction.Insert<?> insert2 = createDeepInsert("single", tag2, null, parentInsert2);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		entity = executor.execute(aggregateChange);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentNoIdList) //
					.extracting(c -> c.single.id) //
					.containsExactly(2, 3); //
		});
	}

	@Test // DATAJDBC-291
	public void setIdForEmbeddedDeepReference() {

		contentNoId = contentNoId2.withSingle(tag1);
		entity = entity.withEmbedded(contentNoId);

		DbAction.Insert<?> parentInsert = createInsert("embedded.single", tag1, null);

		MutableAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.addAction(rootInsert);
		aggregateChange.addAction(parentInsert);

		entity = executor.execute(aggregateChange);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.embedded.single.id).isEqualTo(2);
	}

	private static Map<String, Content> createContentMap(Object... keysAndValues) {

		Map<String, Content> contentMap = new HashMap<>();

		for (int i = 0; i < keysAndValues.length; i += 2) {
			contentMap.put((String) keysAndValues[i], (Content) keysAndValues[i + 1]);
		}
		return unmodifiableMap(contentMap);
	}

	private static Map<String, Tag> createTagMap(Object... keysAndValues) {

		Map<String, Tag> contentMap = new HashMap<>();

		for (int i = 0; i < keysAndValues.length; i += 2) {
			contentMap.put((String) keysAndValues[i], (Tag) keysAndValues[i + 1]);
		}
		return unmodifiableMap(contentMap);
	}

	DbAction.Insert<?> createInsert(String propertyName, Object value, @Nullable Object key) {

		DbAction.Insert<Object> insert = new DbAction.Insert<>(value,
				context.getPersistentPropertyPath(propertyName, DummyEntity.class), rootInsert,
				singletonMap(toPath(propertyName), key), IdValueSource.GENERATED);

		return insert;
	}

	DbAction.Insert<?> createDeepInsert(String propertyName, Object value, Object key,
			@Nullable DbAction.Insert<?> parentInsert) {

		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = toPath(
				parentInsert.getPropertyPath().toDotPath() + "." + propertyName);
		DbAction.Insert<Object> insert = new DbAction.Insert<>(value, propertyPath, parentInsert,
				singletonMap(propertyPath, key), IdValueSource.GENERATED);

		return insert;
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	@Value
	@With
	@AllArgsConstructor
	private static class DummyEntity {

		@Id Integer rootId;
		Content single;
		Set<Content> contentSet;
		List<Content> contentList;
		Map<String, Content> contentMap;
		List<ContentNoId> contentNoIdList;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL, prefix = "fooBar") ContentNoId embedded;

		DummyEntity() {

			rootId = null;
			single = null;
			contentSet = emptySet();
			contentList = emptyList();
			contentMap = emptyMap();
			contentNoIdList = emptyList();
			embedded = new ContentNoId();
		}
	}

	@Value
	@With
	@AllArgsConstructor
	private static class Content {

		@Id Integer id;
		Tag single;
		Set<Tag> tagSet;
		List<Tag> tagList;
		Map<String, Tag> tagMap;

		Content() {

			id = null;
			single = null;
			tagSet = emptySet();
			tagList = emptyList();
			tagMap = emptyMap();
		}
	}

	@Value
	@With
	@AllArgsConstructor
	private static class ContentNoId {
		@Column("single") Tag single;
		Set<Tag> tagSet;
		List<Tag> tagList;
		Map<String, Tag> tagMap;

		ContentNoId() {

			single = null;
			tagSet = emptySet();
			tagList = emptyList();
			tagMap = emptyMap();
		}
	}

	@Value
	@With
	@AllArgsConstructor
	private static class Tag {

		@Id Integer id;

		String name;

		Tag(String name) {
			id = null;
			this.name = name;
		}
	}
}
