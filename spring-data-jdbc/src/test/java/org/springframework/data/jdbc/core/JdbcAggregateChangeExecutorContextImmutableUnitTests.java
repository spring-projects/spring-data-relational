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

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcIdentifierBuilder;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Test for the {@link JdbcAggregateChangeExecutionContext} when operating on immutable classes.
 *
 * @author Jens Schauder
 * @author Chirag Tailor
 */
public class JdbcAggregateChangeExecutorContextImmutableUnitTests {

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

		// note that the root entity isn't the original one, but a new instance with the version set.
		when(accessStrategy.insert(any(DummyEntity.class), eq(DummyEntity.class), eq(Identifier.empty()),
				eq(IdValueSource.GENERATED))).thenReturn(23L);

		executionContext.executeInsertRoot(new DbAction.InsertRoot<>(root, IdValueSource.GENERATED));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).hasSize(1);
		DummyEntity newRoot = newRoots.get(0);
		assertThat(newRoot.id).isEqualTo(23L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChild() {

		Content content = new Content();

		when(accessStrategy.insert(any(DummyEntity.class), eq(DummyEntity.class), eq(Identifier.empty()),
				eq(IdValueSource.GENERATED))).thenReturn(23L);
		when(accessStrategy.insert(any(Content.class), eq(Content.class), eq(createBackRef(23L)),
				eq(IdValueSource.GENERATED))).thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "content", content, null));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).hasSize(1);
		DummyEntity newRoot = newRoots.get(0);
		assertThat(newRoot.id).isEqualTo(23L);

		assertThat(newRoot.content.id).isEqualTo(24L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChildInList() {

		Content content = new Content();

		when(accessStrategy.insert(any(DummyEntity.class), eq(DummyEntity.class), eq(Identifier.empty()),
				eq(IdValueSource.GENERATED))).thenReturn(23L);
		when(accessStrategy.insert(eq(content), eq(Content.class), any(Identifier.class), eq(IdValueSource.GENERATED)))
				.thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "list", content, 1));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).hasSize(1);
		DummyEntity newRoot = newRoots.get(0);
		assertThat(newRoot.id).isEqualTo(23L);

		assertThat(newRoot.list.get(0).id).isEqualTo(24L);
	}

	@Test // GH-537
	void populatesIdsIfNecessaryForAllRootsThatWereProcessed() {

		DummyEntity root1 = new DummyEntity().withId(123L);
		when(accessStrategy.update(root1, DummyEntity.class)).thenReturn(true);
		DbAction.UpdateRoot<DummyEntity> rootUpdate1 = new DbAction.UpdateRoot<>(root1, null);
		executionContext.executeUpdateRoot(rootUpdate1);
		Content content1 = new Content();
		when(accessStrategy.insert(content1, Content.class, createBackRef(123L), IdValueSource.GENERATED)).thenReturn(11L);
		executionContext.executeInsert(createInsert(rootUpdate1, "content", content1, null));

		DummyEntity root2 = new DummyEntity();
		DbAction.InsertRoot<DummyEntity> rootInsert2 = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		when(accessStrategy.insert(root2, DummyEntity.class, Identifier.empty(), IdValueSource.GENERATED)).thenReturn(456L);
		executionContext.executeInsertRoot(rootInsert2);
		Content content2 = new Content();
		when(accessStrategy.insert(content2, Content.class, createBackRef(456L), IdValueSource.GENERATED)).thenReturn(12L);
		executionContext.executeInsert(createInsert(rootInsert2, "content", content2, null));

		List<DummyEntity> newRoots = executionContext.populateIdsIfNecessary();

		assertThat(newRoots).hasSize(2);
		DummyEntity newRoot1 = newRoots.get(0);
		assertThat(newRoot1.id).isEqualTo(123L);
		assertThat(newRoot1.content.id).isEqualTo(11L);
		DummyEntity newRoot2 = newRoots.get(1);
		assertThat(newRoot2.id).isEqualTo(456L);
		assertThat(newRoot2.content.id).isEqualTo(12L);
	}

	DbAction.Insert<?> createInsert(DbAction.WithEntity<?> parent, String propertyName, Object value,
			@Nullable Object key) {

		return new DbAction.Insert<>(value, getPersistentPropertyPath(propertyName), parent,
				key == null ? emptyMap() : singletonMap(toPath(propertyName), key), IdValueSource.GENERATED);
	}

	AggregatePath toAggregatePath(String path) {
		return context.getAggregatePath(getPersistentPropertyPath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> getPersistentPropertyPath(String propertyName) {
		return context.getPersistentPropertyPath(propertyName, DummyEntity.class);
	}

	Identifier createBackRef(long value) {
		return JdbcIdentifierBuilder.forBackReferences(converter, toAggregatePath("content"), value).build();
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	private static final class DummyEntity {

		@Id
		private final Long id;
		@Version
		private final long version;

		private final Content content;

		private final List<Content> list;

		DummyEntity() {

			id = null;
			version = 0;
			content = null;
			list = null;
		}

		public DummyEntity(Long id, long version, Content content, List<Content> list) {
			this.id = id;
			this.version = version;
			this.content = content;
			this.list = list;
		}

		public Long getId() {
			return this.id;
		}

		public long getVersion() {
			return this.version;
		}

		public Content getContent() {
			return this.content;
		}

		public List<Content> getList() {
			return this.list;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof final DummyEntity other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			if (this.getVersion() != other.getVersion()) return false;
			final Object this$content = this.getContent();
			final Object other$content = other.getContent();
			if (!Objects.equals(this$content, other$content))
				return false;
			final Object this$list = this.getList();
			final Object other$list = other.getList();
			return Objects.equals(this$list, other$list);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final long $version = this.getVersion();
			result = result * PRIME + (int) ($version >>> 32 ^ $version);
			final Object $content = this.getContent();
			result = result * PRIME + ($content == null ? 43 : $content.hashCode());
			final Object $list = this.getList();
			result = result * PRIME + ($list == null ? 43 : $list.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcAggregateChangeExecutorContextImmutableUnitTests.DummyEntity(id=" + this.getId() + ", version=" + this.getVersion() + ", content=" + this.getContent() + ", list=" + this.getList() + ")";
		}

		public DummyEntity withId(Long id) {
			return this.id == id ? this : new DummyEntity(id, this.version, this.content, this.list);
		}

		public DummyEntity withVersion(long version) {
			return this.version == version ? this : new DummyEntity(this.id, version, this.content, this.list);
		}

		public DummyEntity withContent(Content content) {
			return this.content == content ? this : new DummyEntity(this.id, this.version, content, this.list);
		}

		public DummyEntity withList(List<Content> list) {
			return this.list == list ? this : new DummyEntity(this.id, this.version, this.content, list);
		}
	}

	private static final class Content {
		@Id
		private final Long id;

		Content() {
			id = null;
		}

		public Content(Long id) {
			this.id = id;
		}

		public Long getId() {
			return this.id;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof final Content other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			return Objects.equals(this$id, other$id);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			return result;
		}

		public String toString() {
			return "JdbcAggregateChangeExecutorContextImmutableUnitTests.Content(id=" + this.getId() + ")";
		}

		public Content withId(Long id) {
			return this.id == id ? this : new Content(id);
		}
	}

}
