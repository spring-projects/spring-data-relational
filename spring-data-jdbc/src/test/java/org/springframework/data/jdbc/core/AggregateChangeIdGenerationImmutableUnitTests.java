/*
 * Copyright 2018-2024 the original author or authors.
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
import org.springframework.data.relational.core.conversion.RootAggregateChange;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

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
	DbAction.WithRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(entity, IdValueSource.GENERATED);

	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

	AggregateChangeExecutor executor = new AggregateChangeExecutor(converter, accessStrategy);

	@Test // DATAJDBC-291
	public void singleRoot() {

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

		assertThat(entity.rootId).isEqualTo(1);
	}

	@Test // DATAJDBC-291
	public void simpleReference() {

		entity = entity.withSingle(content);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("single", content, null));

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.single.id).isEqualTo(2);
		});
	}

	@Test // DATAJDBC-291
	public void listReference() {

		entity = entity.withContentList(asList(content, content2));

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("contentList", content, 0));
		aggregateChange.addAction(createInsert("contentList", content2, 1));

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

		assertSoftly(softly -> {

			softly.assertThat(entity.rootId).isEqualTo(1);
			softly.assertThat(entity.contentList).extracting(c -> c.id).containsExactly(2, 3);
		});
	}

	@Test // DATAJDBC-291
	public void mapReference() {

		entity = entity.withContentMap(createContentMap("a", content, "b", content2));

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(createInsert("contentMap", content, "a"));
		aggregateChange.addAction(createInsert("contentMap", content2, "b"));

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

		assertThat(entity.rootId).isEqualTo(1);
		assertThat(entity.contentMap.values()).extracting(c -> c.id).containsExactly(2, 3);
	}

	@Test // DATAJDBC-291
	public void setIdForDeepReference() {

		content = content.withSingle(tag1);
		entity = entity.withSingle(content);

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert = createDeepInsert("single", tag1, null, parentInsert);

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);
		aggregateChange.addAction(insert3);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert1);
		aggregateChange.addAction(parentInsert2);
		aggregateChange.addAction(insert1);
		aggregateChange.addAction(insert2);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		RootAggregateChange<DummyEntity> aggregateChange = MutableAggregateChange.forSave(entity);
		aggregateChange.setRootAction(rootInsert);
		aggregateChange.addAction(parentInsert);

		List<DummyEntity> result = executor.executeSave(aggregateChange);
		entity = result.get(0);

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

		return new DbAction.Insert<>(value,
				context.getPersistentPropertyPath(propertyName, DummyEntity.class), rootInsert,
				singletonMap(toPath(propertyName), key), IdValueSource.GENERATED);
	}

	DbAction.Insert<?> createDeepInsert(String propertyName, Object value, Object key,
										@Nullable DbAction.Insert<?> parentInsert) {

		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = toPath(
				parentInsert.getPropertyPath().toDotPath() + "." + propertyName);

		return new DbAction.Insert<>(value, propertyPath, parentInsert,
				singletonMap(propertyPath, key), IdValueSource.GENERATED);
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	private static final class DummyEntity {

		@Id
		private final
		Integer rootId;
		private final Content single;
		private final Set<Content> contentSet;
		private final List<Content> contentList;
		private final Map<String, Content> contentMap;
		private final List<ContentNoId> contentNoIdList;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL, prefix = "fooBar")
		private final
		ContentNoId embedded;

		DummyEntity() {

			rootId = null;
			single = null;
			contentSet = emptySet();
			contentList = emptyList();
			contentMap = emptyMap();
			contentNoIdList = emptyList();
			embedded = new ContentNoId();
		}

		public DummyEntity(Integer rootId, Content single, Set<Content> contentSet, List<Content> contentList, Map<String, Content> contentMap, List<ContentNoId> contentNoIdList, ContentNoId embedded) {
			this.rootId = rootId;
			this.single = single;
			this.contentSet = contentSet;
			this.contentList = contentList;
			this.contentMap = contentMap;
			this.contentNoIdList = contentNoIdList;
			this.embedded = embedded;
		}

		public Integer getRootId() {
			return this.rootId;
		}

		public Content getSingle() {
			return this.single;
		}

		public Set<Content> getContentSet() {
			return this.contentSet;
		}

		public List<Content> getContentList() {
			return this.contentList;
		}

		public Map<String, Content> getContentMap() {
			return this.contentMap;
		}

		public List<ContentNoId> getContentNoIdList() {
			return this.contentNoIdList;
		}

		public ContentNoId getEmbedded() {
			return this.embedded;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof DummyEntity other)) return false;
			final Object this$rootId = this.getRootId();
			final Object other$rootId = other.getRootId();
			if (this$rootId == null ? other$rootId != null : !this$rootId.equals(other$rootId)) return false;
			final Object this$single = this.getSingle();
			final Object other$single = other.getSingle();
			if (this$single == null ? other$single != null : !this$single.equals(other$single)) return false;
			final Object this$contentSet = this.getContentSet();
			final Object other$contentSet = other.getContentSet();
			if (this$contentSet == null ? other$contentSet != null : !this$contentSet.equals(other$contentSet))
				return false;
			final Object this$contentList = this.getContentList();
			final Object other$contentList = other.getContentList();
			if (this$contentList == null ? other$contentList != null : !this$contentList.equals(other$contentList))
				return false;
			final Object this$contentMap = this.getContentMap();
			final Object other$contentMap = other.getContentMap();
			if (this$contentMap == null ? other$contentMap != null : !this$contentMap.equals(other$contentMap))
				return false;
			final Object this$contentNoIdList = this.getContentNoIdList();
			final Object other$contentNoIdList = other.getContentNoIdList();
			if (this$contentNoIdList == null ? other$contentNoIdList != null : !this$contentNoIdList.equals(other$contentNoIdList))
				return false;
			final Object this$embedded = this.getEmbedded();
			final Object other$embedded = other.getEmbedded();
			if (this$embedded == null ? other$embedded != null : !this$embedded.equals(other$embedded)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $rootId = this.getRootId();
			result = result * PRIME + ($rootId == null ? 43 : $rootId.hashCode());
			final Object $single = this.getSingle();
			result = result * PRIME + ($single == null ? 43 : $single.hashCode());
			final Object $contentSet = this.getContentSet();
			result = result * PRIME + ($contentSet == null ? 43 : $contentSet.hashCode());
			final Object $contentList = this.getContentList();
			result = result * PRIME + ($contentList == null ? 43 : $contentList.hashCode());
			final Object $contentMap = this.getContentMap();
			result = result * PRIME + ($contentMap == null ? 43 : $contentMap.hashCode());
			final Object $contentNoIdList = this.getContentNoIdList();
			result = result * PRIME + ($contentNoIdList == null ? 43 : $contentNoIdList.hashCode());
			final Object $embedded = this.getEmbedded();
			result = result * PRIME + ($embedded == null ? 43 : $embedded.hashCode());
			return result;
		}

		public String toString() {
			return "AggregateChangeIdGenerationImmutableUnitTests.DummyEntity(rootId=" + this.getRootId() + ", single=" + this.getSingle() + ", contentSet=" + this.getContentSet() + ", contentList=" + this.getContentList() + ", contentMap=" + this.getContentMap() + ", contentNoIdList=" + this.getContentNoIdList() + ", embedded=" + this.getEmbedded() + ")";
		}

		public DummyEntity withRootId(Integer rootId) {
			return this.rootId == rootId ? this : new DummyEntity(rootId, this.single, this.contentSet, this.contentList, this.contentMap, this.contentNoIdList, this.embedded);
		}

		public DummyEntity withSingle(Content single) {
			return this.single == single ? this : new DummyEntity(this.rootId, single, this.contentSet, this.contentList, this.contentMap, this.contentNoIdList, this.embedded);
		}

		public DummyEntity withContentSet(Set<Content> contentSet) {
			return this.contentSet == contentSet ? this : new DummyEntity(this.rootId, this.single, contentSet, this.contentList, this.contentMap, this.contentNoIdList, this.embedded);
		}

		public DummyEntity withContentList(List<Content> contentList) {
			return this.contentList == contentList ? this : new DummyEntity(this.rootId, this.single, this.contentSet, contentList, this.contentMap, this.contentNoIdList, this.embedded);
		}

		public DummyEntity withContentMap(Map<String, Content> contentMap) {
			return this.contentMap == contentMap ? this : new DummyEntity(this.rootId, this.single, this.contentSet, this.contentList, contentMap, this.contentNoIdList, this.embedded);
		}

		public DummyEntity withContentNoIdList(List<ContentNoId> contentNoIdList) {
			return this.contentNoIdList == contentNoIdList ? this : new DummyEntity(this.rootId, this.single, this.contentSet, this.contentList, this.contentMap, contentNoIdList, this.embedded);
		}

		public DummyEntity withEmbedded(ContentNoId embedded) {
			return this.embedded == embedded ? this : new DummyEntity(this.rootId, this.single, this.contentSet, this.contentList, this.contentMap, this.contentNoIdList, embedded);
		}
	}

	private static final class Content {

		@Id
		private final
		Integer id;
		private final Tag single;
		private final Set<Tag> tagSet;
		private final List<Tag> tagList;
		private final Map<String, Tag> tagMap;

		Content() {

			id = null;
			single = null;
			tagSet = emptySet();
			tagList = emptyList();
			tagMap = emptyMap();
		}

		public Content(Integer id, Tag single, Set<Tag> tagSet, List<Tag> tagList, Map<String, Tag> tagMap) {
			this.id = id;
			this.single = single;
			this.tagSet = tagSet;
			this.tagList = tagList;
			this.tagMap = tagMap;
		}

		public Integer getId() {
			return this.id;
		}

		public Tag getSingle() {
			return this.single;
		}

		public Set<Tag> getTagSet() {
			return this.tagSet;
		}

		public List<Tag> getTagList() {
			return this.tagList;
		}

		public Map<String, Tag> getTagMap() {
			return this.tagMap;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Content other)) return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$single = this.getSingle();
			final Object other$single = other.getSingle();
			if (this$single == null ? other$single != null : !this$single.equals(other$single)) return false;
			final Object this$tagSet = this.getTagSet();
			final Object other$tagSet = other.getTagSet();
			if (this$tagSet == null ? other$tagSet != null : !this$tagSet.equals(other$tagSet)) return false;
			final Object this$tagList = this.getTagList();
			final Object other$tagList = other.getTagList();
			if (this$tagList == null ? other$tagList != null : !this$tagList.equals(other$tagList)) return false;
			final Object this$tagMap = this.getTagMap();
			final Object other$tagMap = other.getTagMap();
			if (this$tagMap == null ? other$tagMap != null : !this$tagMap.equals(other$tagMap)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $single = this.getSingle();
			result = result * PRIME + ($single == null ? 43 : $single.hashCode());
			final Object $tagSet = this.getTagSet();
			result = result * PRIME + ($tagSet == null ? 43 : $tagSet.hashCode());
			final Object $tagList = this.getTagList();
			result = result * PRIME + ($tagList == null ? 43 : $tagList.hashCode());
			final Object $tagMap = this.getTagMap();
			result = result * PRIME + ($tagMap == null ? 43 : $tagMap.hashCode());
			return result;
		}

		public String toString() {
			return "AggregateChangeIdGenerationImmutableUnitTests.Content(id=" + this.getId() + ", single=" + this.getSingle() + ", tagSet=" + this.getTagSet() + ", tagList=" + this.getTagList() + ", tagMap=" + this.getTagMap() + ")";
		}

		public Content withId(Integer id) {
			return this.id == id ? this : new Content(id, this.single, this.tagSet, this.tagList, this.tagMap);
		}

		public Content withSingle(Tag single) {
			return this.single == single ? this : new Content(this.id, single, this.tagSet, this.tagList, this.tagMap);
		}

		public Content withTagSet(Set<Tag> tagSet) {
			return this.tagSet == tagSet ? this : new Content(this.id, this.single, tagSet, this.tagList, this.tagMap);
		}

		public Content withTagList(List<Tag> tagList) {
			return this.tagList == tagList ? this : new Content(this.id, this.single, this.tagSet, tagList, this.tagMap);
		}

		public Content withTagMap(Map<String, Tag> tagMap) {
			return this.tagMap == tagMap ? this : new Content(this.id, this.single, this.tagSet, this.tagList, tagMap);
		}
	}

	private static final class ContentNoId {
		@Column("single")
		private final
		Tag single;
		private final Set<Tag> tagSet;
		private final List<Tag> tagList;
		private final Map<String, Tag> tagMap;

		ContentNoId() {

			single = null;
			tagSet = emptySet();
			tagList = emptyList();
			tagMap = emptyMap();
		}

		public ContentNoId(Tag single, Set<Tag> tagSet, List<Tag> tagList, Map<String, Tag> tagMap) {
			this.single = single;
			this.tagSet = tagSet;
			this.tagList = tagList;
			this.tagMap = tagMap;
		}

		public Tag getSingle() {
			return this.single;
		}

		public Set<Tag> getTagSet() {
			return this.tagSet;
		}

		public List<Tag> getTagList() {
			return this.tagList;
		}

		public Map<String, Tag> getTagMap() {
			return this.tagMap;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof ContentNoId other)) return false;
			final Object this$single = this.getSingle();
			final Object other$single = other.getSingle();
			if (this$single == null ? other$single != null : !this$single.equals(other$single)) return false;
			final Object this$tagSet = this.getTagSet();
			final Object other$tagSet = other.getTagSet();
			if (this$tagSet == null ? other$tagSet != null : !this$tagSet.equals(other$tagSet)) return false;
			final Object this$tagList = this.getTagList();
			final Object other$tagList = other.getTagList();
			if (this$tagList == null ? other$tagList != null : !this$tagList.equals(other$tagList)) return false;
			final Object this$tagMap = this.getTagMap();
			final Object other$tagMap = other.getTagMap();
			if (this$tagMap == null ? other$tagMap != null : !this$tagMap.equals(other$tagMap)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $single = this.getSingle();
			result = result * PRIME + ($single == null ? 43 : $single.hashCode());
			final Object $tagSet = this.getTagSet();
			result = result * PRIME + ($tagSet == null ? 43 : $tagSet.hashCode());
			final Object $tagList = this.getTagList();
			result = result * PRIME + ($tagList == null ? 43 : $tagList.hashCode());
			final Object $tagMap = this.getTagMap();
			result = result * PRIME + ($tagMap == null ? 43 : $tagMap.hashCode());
			return result;
		}

		public String toString() {
			return "AggregateChangeIdGenerationImmutableUnitTests.ContentNoId(single=" + this.getSingle() + ", tagSet=" + this.getTagSet() + ", tagList=" + this.getTagList() + ", tagMap=" + this.getTagMap() + ")";
		}

		public ContentNoId withSingle(Tag single) {
			return this.single == single ? this : new ContentNoId(single, this.tagSet, this.tagList, this.tagMap);
		}

		public ContentNoId withTagSet(Set<Tag> tagSet) {
			return this.tagSet == tagSet ? this : new ContentNoId(this.single, tagSet, this.tagList, this.tagMap);
		}

		public ContentNoId withTagList(List<Tag> tagList) {
			return this.tagList == tagList ? this : new ContentNoId(this.single, this.tagSet, tagList, this.tagMap);
		}

		public ContentNoId withTagMap(Map<String, Tag> tagMap) {
			return this.tagMap == tagMap ? this : new ContentNoId(this.single, this.tagSet, this.tagList, tagMap);
		}
	}

	private static final class Tag {

		@Id
		private final
		Integer id;

		private final String name;

		Tag(String name) {
			id = null;
			this.name = name;
		}

		public Tag(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Tag other)) return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			return result;
		}

		public String toString() {
			return "AggregateChangeIdGenerationImmutableUnitTests.Tag(id=" + this.getId() + ", name=" + this.getName() + ")";
		}

		public Tag withId(Integer id) {
			return this.id == id ? this : new Tag(id, this.name);
		}

		public Tag withName(String name) {
			return this.name == name ? this : new Tag(this.id, name);
		}
	}
}
