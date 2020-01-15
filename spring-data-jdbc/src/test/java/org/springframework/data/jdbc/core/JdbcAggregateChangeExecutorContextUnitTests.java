/*
 * Copyright 2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.lang.Nullable;

public class JdbcAggregateChangeExecutorContextUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	JdbcConverter converter = new BasicJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

	AggregateChangeExecutor executor = new AggregateChangeExecutor(converter, accessStrategy);
	JdbcAggregateChangeExecutionContext executionContext = new JdbcAggregateChangeExecutionContext(converter,
			accessStrategy);

	DummyEntity root = new DummyEntity();

	@Test // DATAJDBC-453
	public void rootOfEmptySetOfActionsisNull() {

		Object root = executionContext.populateIdsIfNecessary();

		assertThat(root).isNull();
	}

	@Test // DATAJDBC-453
	public void afterInsertRootIdAndVersionMaybeUpdated() {

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty())).thenReturn(23L);

		executionContext.executeInsertRoot(new DbAction.InsertRoot<>(root));

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNull();
		assertThat(root.id).isEqualTo(23L);

		executionContext.populateRootVersionIfNecessary(root);

		assertThat(root.version).isEqualTo(1);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChild() {

		Content content = new Content();

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty())).thenReturn(23L);
		when(accessStrategy.insert(content, Content.class, createBackRef())).thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "content", content, null));

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNull();
		assertThat(root.id).isEqualTo(23L);

		assertThat(content.id).isEqualTo(24L);
	}

	@Test // DATAJDBC-453
	public void idGenerationOfChildInList() {

		Content content = new Content();

		when(accessStrategy.insert(root, DummyEntity.class, Identifier.empty())).thenReturn(23L);
		when(accessStrategy.insert(eq(content), eq(Content.class), any(Identifier.class))).thenReturn(24L);

		DbAction.InsertRoot<DummyEntity> rootInsert = new DbAction.InsertRoot<>(root);
		executionContext.executeInsertRoot(rootInsert);
		executionContext.executeInsert(createInsert(rootInsert, "list", content, 1));

		DummyEntity newRoot = executionContext.populateIdsIfNecessary();

		assertThat(newRoot).isNull();
		assertThat(root.id).isEqualTo(23L);

		assertThat(content.id).isEqualTo(24L);
	}

	DbAction.Insert<?> createInsert(DbAction.WithEntity<?> parent, String propertyName, Object value,
			@Nullable Object key) {

		DbAction.Insert<Object> insert = new DbAction.Insert<>(value, getPersistentPropertyPath(propertyName), parent,
				key == null ? emptyMap() : singletonMap(toPath(propertyName), key));

		return insert;
	}

	PersistentPropertyPathExtension toPathExt(String path) {
		return new PersistentPropertyPathExtension(context, getPersistentPropertyPath(path));
	}

	@NotNull
	PersistentPropertyPath<RelationalPersistentProperty> getPersistentPropertyPath(String propertyName) {
		return context.getPersistentPropertyPath(propertyName, DummyEntity.class);
	}

	@NotNull
	Identifier createBackRef() {
		return JdbcIdentifierBuilder.forBackReferences(converter, toPathExt("content"), 23L).build();
	}

	PersistentPropertyPath<RelationalPersistentProperty> toPath(String path) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> true);

		return persistentPropertyPaths.filter(p -> p.toDotPath().equals(path)).stream().findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No matching path found"));
	}

	private static class DummyEntity {

		@Id Long id;
		@Version long version;

		Content content;

		List<Content> list = new ArrayList<>();
	}

	private static class Content {
		@Id Long id;
	}

}
