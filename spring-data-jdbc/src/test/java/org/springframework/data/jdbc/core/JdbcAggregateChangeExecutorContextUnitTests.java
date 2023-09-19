/*
 * Copyright 2020-2023 the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.jdbc.core.convert.JdbcIdentifierBuilder.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.InsertSubject;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link JdbcAggregateChangeExecutionContext}.
 *
 * @author Jens Schauder
 * @author Umut Erturk
 * @author Chirag Tailor
 */
public class JdbcAggregateChangeExecutorContextUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

	JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
			accessStrategy);

	DummyEntity root = new DummyEntity();

	@Test // DATAJDBC-453
	public void afterInsertRootIdMaybeUpdated() {

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(23L);

		executionContext.executeInsertRoot(new DbAction.InsertRoot<>(root, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(23L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChild() {

		Content content = new Content();

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(23L);
		when(accessStrategy.insert(content, Content.class, createBackRef(23L), IdValueSource.GENERATED)).thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "content", content, null, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(23L);

		assertThat(content.id).isEqualTo(24L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChildInList() {

		Content content = new Content();

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(23L);
		when(accessStrategy.insert(eq(content), eq(Content.class), any(Identifier.class), eq(IdValueSource.GENERATED)))
				.thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "list", content, 1, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(23L);

		assertThat(content.id).isEqualTo(24L);
	}

	@Test // GH-1159
	void batchInsertOperation_withGeneratedIds() {

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(123L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);

		Content content = new Content();
		Identifier identifier = Identifier.empty().withPart(SqlIdentifier.quoted("DUMMY_ENTITY"), 123L, Long.class)
				.withPart(SqlIdentifier.quoted("DUMMY_ENTITY_KEY"), 0, Integer.class);
		when(accessStrategy.insert(singletonList(InsertSubject.describedBy(content, identifier)), Content.class,
				IdValueSource.GENERATED)).thenReturn(new Object[] { 456L });
		DbAction.BatchInsert<?> batchInsert = new DbAction.BatchInsert<>(
				singletonList(createInsert(rootInsert, "list", content, 0, IdValueSource.GENERATED)));
		executionContext.executeBatchInsert(batchInsert);

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(123L);
		assertThat(content.id).isEqualTo(456L);
	}

	@Test // GH-1159
	void batchInsertOperation_withoutGeneratedIds() {

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(123L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);

		Content content = new Content();
		Identifier identifier = Identifier.empty().withPart(SqlIdentifier.quoted("DUMMY_ENTITY"), 123L, Long.class)
				.withPart(SqlIdentifier.quoted("DUMMY_ENTITY_KEY"), 0, Integer.class);
		when(accessStrategy.insert(singletonList(InsertSubject.describedBy(content, identifier)), Content.class,
				IdValueSource.PROVIDED)).thenReturn(new Object[] { null });
		DbAction.BatchInsert<?> batchInsert = new DbAction.BatchInsert<>(
				singletonList(createInsert(rootInsert, "list", content, 0, IdValueSource.PROVIDED)));
		executionContext.executeBatchInsert(batchInsert);

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(123L);
		assertThat(content.id).isNull();
	}

	@Test // GH-537
	void batchInsertRootOperation_withGeneratedIds() {

		when(accessStrategy.insert(singletonList(InsertSubject.describedBy(root, Identifier.empty())), DummyEntity.class,
				IdValueSource.GENERATED)).thenReturn(new Object[] { 123L });
		executionContext.executeBatchInsertRoot(
				new DbAction.BatchInsertRoot<>(singletonList(new DbAction.InsertRoot<>(root, IdValueSource.GENERATED))));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(123L);
	}

	@Test // GH-537
	void batchInsertRootOperation_withoutGeneratedIds() {

		when(accessStrategy.insert(singletonList(InsertSubject.describedBy(root, Identifier.empty())), DummyEntity.class,
				IdValueSource.PROVIDED)).thenReturn(new Object[] { null });
		executionContext.executeBatchInsertRoot(
				new DbAction.BatchInsertRoot<>(singletonList(new DbAction.InsertRoot<>(root, IdValueSource.PROVIDED))));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isNull();
	}

	@Test // GH-1201
	void updates_whenReferencesWithImmutableIdAreInserted() {

		root.id = 123L;
		when(accessStrategy.update(root, DummyEntity.class)).thenReturn(true);
		DbAction.UpdateRoot<DummyEntity> rootUpdate = new DbAction.UpdateRoot<>(root, null);
		executionContext.executeUpdateRoot(rootUpdate);

		ContentImmutableId contentImmutableId = new ContentImmutableId(null);
		root.contentImmutableId = contentImmutableId;
		Identifier identifier = Identifier.empty().withPart(SqlIdentifier.quoted("DUMMY_ENTITY"), 123L, Long.class);
		when(accessStrategy.insert(contentImmutableId, ContentImmutableId.class, identifier, IdValueSource.GENERATED))
				.thenReturn(456L);
		executionContext.executeInsert(
				createInsert(rootUpdate, "contentImmutableId", contentImmutableId, null, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();
		assertThat(newRoots).containsExactly(root);
		assertThat(root.id).isEqualTo(123L);
		assertThat(root.contentImmutableId.id).isEqualTo(456L);
	}

	@Test // GH-537
	void populatesIdsIfNecessaryForAllRootsThatWereProcessed() {

		DummyEntity root1 = new DummyEntity();
		root1.id = 123L;
		when(accessStrategy.update(root1, DummyEntity.class)).thenReturn(true);
		DbAction.UpdateRoot<DummyEntity> rootUpdate1 = new DbAction.UpdateRoot<>(root1, null);
		executionContext.executeUpdateRoot(rootUpdate1);
		Content content1 = new Content();
		when(accessStrategy.insert(content1, Content.class, createBackRef(123L), IdValueSource.GENERATED)).thenReturn(11L);
		executionContext.executeInsert(createInsert(rootUpdate1, "content", content1, null, IdValueSource.GENERATED));

		DummyEntity root2 = new DummyEntity();
		DbAction.InsertRoot<DummyEntity> rootInsert2 = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		when(accessStrategy.insert(root2, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(456L);
		executionContext.executeInsertRoot(rootInsert2);
		Content content2 = new Content();
		when(accessStrategy.insert(content2, Content.class, createBackRef(456L), IdValueSource.GENERATED)).thenReturn(12L);
		executionContext.executeInsert(createInsert(rootInsert2, "content", content2, null, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).containsExactly(root1, root2);
		assertThat(root1.id).isEqualTo(123L);
		assertThat(content1.id).isEqualTo(11L);
		assertThat(root2.id).isEqualTo(456L);
		assertThat(content2.id).isEqualTo(12L);
	}

	DbAction.Insert<?> createInsert(DbAction.WithEntity<?> parent, String propertyName, Object value,
			@Nullable Object key, IdValueSource idValueSource) {

		return new DbAction.Insert<>(value, getPersistentPropertyPath(propertyName), parent,
				key == null ? emptyMap() : singletonMap(toPath(propertyName), key), idValueSource);
	}

	AggregatePath toAggregatePath(String path) {
		return context.getAggregatePath(getPersistentPropertyPath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> getPersistentPropertyPath(String propertyName) {
		return context.getPersistentPropertyPath(propertyName, DummyEntity.class);
	}

	Identifier createBackRef(long value) {
		return forBackReferences(converter, toAggregatePath("content"), value).build();
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	@SuppressWarnings("unused")
	private static class DummyEntity {

		@Id Long id;

		Content content;

		ContentImmutableId contentImmutableId;

		List<Content> list = new ArrayList<>();
	}

	private static class Content {
		@Id Long id;
	}

	record ContentImmutableId(@Id Long id) {
	}

}
