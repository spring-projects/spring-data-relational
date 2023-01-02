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

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.With;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcIdentifierBuilder;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

public class JdbcAggregateChangeExecutorContextImmutableUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	JdbcConverter converter = new BasicJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

	JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
			accessStrategy);

	DummyEntity root = new DummyEntity();

	@Test // DATAJDBC-453
	public void rootOfEmptySetOfActionsisNull() {

		Object root = executionContext.populateIdsIfNecessary();

		assertThat(root).isNull();
	}

	@Test // DATAJDBC-453
	public void afterInsertRootIdMaybeUpdated() {

		// note that the root entity isn't the original one, but a new instance with the version set.
		when(accessStrategy.insert(any(DummyEntity.class), eq(DummyEntity.class), eq(Identifier.empty()),
				eq(IdValueSource.GENERATED))).thenReturn(23L);

		executionContext.executeInsertRoot(new DbAction.InsertRoot<>(root, IdValueSource.GENERATED));

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNotNull();
		assertThat(newRoot.id).isEqualTo(23L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChild() {

		Content content = new Content();

		when(accessStrategy.insert(any(DummyEntity.class), eq(DummyEntity.class), eq(Identifier.empty()),
				eq(IdValueSource.GENERATED))).thenReturn(23L);
		when(accessStrategy.insert(any(Content.class), eq(Content.class), eq(createBackRef()), eq(IdValueSource.GENERATED)))
				.thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "content", content, null));

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNotNull();
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

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNotNull();
		assertThat(newRoot.id).isEqualTo(23L);

		assertThat(newRoot.list.get(0).id).isEqualTo(24L);
	}

	DbAction.Insert<?> createInsert(DbAction.WithEntity<?> parent, String propertyName, Object value,
			@Nullable Object key) {

		return new DbAction.Insert<>(value, getPersistentPropertyPath(propertyName), parent,
				key == null ? emptyMap() : singletonMap(toPath(propertyName), key), IdValueSource.GENERATED);
	}

	PersistentPropertyPathExtension toPathExt(String path) {
		return new PersistentPropertyPathExtension(context, getPersistentPropertyPath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> getPersistentPropertyPath(String propertyName) {
		return context.getPersistentPropertyPath(propertyName, DummyEntity.class);
	}

	Identifier createBackRef() {
		return JdbcIdentifierBuilder.forBackReferences(converter, toPathExt("content"), 23L).build();
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	@Value
	@AllArgsConstructor
	@With
	private static class DummyEntity {

		@Id Long id;
		@Version long version;

		Content content;

		List<Content> list;

		DummyEntity() {

			id = null;
			version = 0;
			content = null;
			list = null;
		}
	}

	@Value
	@AllArgsConstructor
	@With
	private static class Content {
		@Id Long id;

		Content() {
			id = null;
		}
	}

}
