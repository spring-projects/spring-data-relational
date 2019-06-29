/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for the {@link AggregateChange}.
 *
 * @author Jens Schauder
 */
public class AggregateChangeUnitTests {

	DummyEntity entity = new DummyEntity();
	Content content = new Content();
	Tag tag = new Tag();

	RelationalMappingContext context = new RelationalMappingContext();
	RelationalConverter converter = new BasicRelationalConverter(context);

	PersistentPropertyAccessor<DummyEntity> propertyAccessor = context.getRequiredPersistentEntity(DummyEntity.class)
			.getPropertyAccessor(entity);
	PersistentPropertyAccessor<Content> contentPropertyAccessor = context.getRequiredPersistentEntity(Content.class)
		.getPropertyAccessor(content);
	Object id = 23;

	DbAction.WithEntity<?> rootInsert = new DbAction.InsertRoot<>(entity);

	DbAction.Insert<?> createInsert(String propertyName, Object value, Object key) {

		DbAction.Insert<Object> insert = new DbAction.Insert<>(value,
				context.getPersistentPropertyPath(propertyName, DummyEntity.class), rootInsert);
		insert.getQualifiers().put(toPath(propertyName, DummyEntity.class), key);

		return insert;
	}

	DbAction.Insert<?> createDeepInsert(String propertyName, Object value, Object key, DbAction.Insert<?> parentInsert) {
		DbAction.Insert<Object> insert = new DbAction.Insert<>(value, toPath(entity, value), parentInsert);
		insert.getQualifiers().put(toPath(parentInsert.getPropertyPath().toDotPath() + "." + propertyName, DummyEntity.class), key);
		return insert;
	}

	@Test // DATAJDBC-241
	public void setIdForSimpleReference() {

		entity.single = content;

		DbAction.Insert<?> insert = createInsert("single", content, null);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		DummyEntity result = propertyAccessor.getBean();

		assertThat(result.single.id).isEqualTo(id);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementSet() {

		entity.contentSet.add(content);

		DbAction.Insert<?> insert = createInsert("contentSet", content, null);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentSet).isNotNull();
		assertThat(result.contentSet).extracting(c -> c == null ? "null" : c.id).containsExactlyInAnyOrder(23);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementList() {

		entity.contentList.add(content);

		DbAction.Insert<?> insert = createInsert("contentList", content, 0);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentList).extracting(c -> c.id).containsExactlyInAnyOrder(23);
	}

	@Test // DATAJDBC-241
	public void setIdForSingleElementMap() {

		entity.contentMap.put("one", content);

		DbAction.Insert<?> insert = createInsert("contentMap", content, "one");

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		DummyEntity result = propertyAccessor.getBean();
		assertThat(result.contentMap.entrySet()).extracting(e -> e.getKey(), e -> e.getValue().id)
				.containsExactlyInAnyOrder(tuple("one", 23));
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepReference() {

		content.single = tag;
		entity.single = content;

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert = createDeepInsert("single", tag, null, parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();

		assertThat(result.single.id).isEqualTo(id);
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepReferenceElementList() {

		content.tagList.add(tag);
		entity.single = content;

		DbAction.Insert<?> parentInsert = createInsert("single", content, null);
		DbAction.Insert<?> insert = createDeepInsert("tagList", tag, 0, parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();
		assertThat(result.tagList).extracting(c -> c.id).containsExactlyInAnyOrder(23);
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepElementSetElementSet() {

		content.tagSet.add(tag);
		entity.contentSet.add(content);

		DbAction.Insert<?> parentInsert = createInsert("contentSet", content, null);
		DbAction.Insert<?> insert = createDeepInsert("tagSet", tag, null, parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();
		assertThat(result.tagSet).isNotNull();
		assertThat(result.tagSet).extracting(c -> c == null ? "null" : c.id).containsExactlyInAnyOrder(23);
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepElementListSingleReference() {

		content.single = tag;
		entity.contentList.add(content);

		DbAction.Insert<?> parentInsert = createInsert("contentList", content, 0);
		DbAction.Insert<?> insert = createDeepInsert("single", tag, null, parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();
		assertThat(result.single.id).isEqualTo(id);
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepElementListElementList() {

		content.tagList.add(tag);
		entity.contentList.add(content);

		DbAction.Insert<?> parentInsert = createInsert("contentList", content, 0);
		DbAction.Insert<?> insert = createDeepInsert("tagList", tag, 0, parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();
		assertThat(result.tagList).extracting(c -> c.id).containsExactlyInAnyOrder(23);
	}

	@Test	// DATAJDBC-291
	public void setIdForDeepElementMapElementMap() {

		content.tagMap.put("one", tag);
		entity.contentMap.put("one", content);

		DbAction.Insert<?> parentInsert = createInsert("contentMap", content, "one");
		DbAction.Insert<?> insert = createDeepInsert("tagMap", tag, "one", parentInsert);

		AggregateChange.setIdOfNonRootEntity(context, converter, propertyAccessor, insert, id);

		Content result = contentPropertyAccessor.getBean();
		assertThat(result.tagMap.entrySet()).extracting(e -> e.getKey(), e -> e.getValue().id)
			.containsExactlyInAnyOrder(tuple("one", 23));
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path, Class source) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(source, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst().orElse(null);
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(DummyEntity root, Object pathValue) {
		// DefaultPersistentPropertyPath is package-public
		return new WritingContext(context, entity, new AggregateChange<>(AggregateChange.Kind.SAVE, DummyEntity.class, root))
			.insert().stream().filter(a -> a instanceof DbAction.Insert).map(DbAction.Insert.class::cast)
			.filter(a -> a.getEntity() == pathValue)
			.map(DbAction.Insert::getPropertyPath)
			.findFirst().orElse(null);
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
}
