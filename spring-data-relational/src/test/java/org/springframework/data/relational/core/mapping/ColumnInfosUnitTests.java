/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for the construction of {@link org.springframework.data.relational.core.mapping.AggregatePath.ColumnInfos}
 *
 * @author Jens Schauder
 */
class ColumnInfosUnitTests {

	static final Table TABLE = Table.create("dummy");
	static final SqlIdentifier ID = SqlIdentifier.quoted("ID");
	RelationalMappingContext context = new RelationalMappingContext();

	@Test // GH-574
	void emptyColumnInfos() {

		AggregatePath.ColumnInfos columnInfos = AggregatePath.ColumnInfos.empty();

		assertThat(columnInfos.isEmpty()).isTrue();
		assertThrows(NoSuchElementException.class, columnInfos::any);
		assertThrows(IllegalStateException.class, columnInfos::unique);
		assertThat(columnInfos.toColumnList(TABLE)).isEmpty();
	}

	@Test // GH-574
	void singleElementColumnInfos() {

		AggregatePath.ColumnInfos columnInfos = basePath(DummyEntity.class).getTableInfo().idColumnInfos();

		assertThat(columnInfos.isEmpty()).isFalse();
		assertThat(columnInfos.any().name()).isEqualTo(ID);
		assertThat(columnInfos.unique().name()).isEqualTo(ID);
		assertThat(columnInfos.toColumnList(TABLE)).containsExactly(TABLE.column(ID));
	}

	@Test // GH-574
	void multiElementColumnInfos() {

		AggregatePath.ColumnInfos columnInfos = basePath(WithCompositeId.class).getTableInfo().idColumnInfos();

		assertThat(columnInfos.isEmpty()).isFalse();
		assertThat(columnInfos.any().name()).isEqualTo(SqlIdentifier.quoted("ONE"));
		assertThrows(IllegalStateException.class, columnInfos::unique);
		assertThat(columnInfos.toColumnList(TABLE)) //
				.containsExactly( //
						TABLE.column(SqlIdentifier.quoted("ONE")), //
						TABLE.column(SqlIdentifier.quoted("TWO")) //
				);

		List<String> collector = new ArrayList<>();
		columnInfos.forEach((ap, ci) -> collector.add(ap.toDotPath() + "+" + ci.name()));
		assertThat(collector).containsExactly("id.one+\"ONE\"", "id.two+\"TWO\"");
	}

	private AggregatePath getPath(Class<?> type, String name) {
		return basePath(type).append(context.getPersistentEntity(type).getPersistentProperty(name));
	}

	private AggregatePath basePath(Class<?> type) {
		return context.getAggregatePath(context.getPersistentEntity(type));
	}

	record DummyEntity(@Id String id, String name) {
	}

	record CompositeId(String one, String two) {
	}

	record WithCompositeId(@Id @Embedded.Nullable CompositeId id, String name) {
	}
}
