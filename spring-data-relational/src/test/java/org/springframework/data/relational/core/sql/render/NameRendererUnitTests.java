/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for the {@link NameRenderer}.
 *
 * @author Jens Schauder
 */
class NameRendererUnitTests {

	RenderContext context = new SimpleRenderContext(NamingStrategies.asIs());

	@Test // GH-1003
	void rendersColumnWithoutTableName() {

		Column column = Column.create("column", Table.create("table"));

		CharSequence rendered = NameRenderer.render(context, column);

		assertThat(rendered).isEqualTo("column");
	}

	@Test // GH-1003, GH-968
	void fullyQualifiedReferenceWithAlias() {

		Column column = Column.aliased("col", Table.aliased("table", "tab_alias"), "col_alias");

		CharSequence rendered = NameRenderer.fullyQualifiedReference(context, column);

		assertThat(rendered).isEqualTo("col_alias");
	}

	@Test // GH-1003, GH-968
	void fullyQualifiedReference() {

		Column column = Table.aliased("table", "tab_alias").column("col");

		CharSequence rendered = NameRenderer.fullyQualifiedReference(context, column);

		assertThat(rendered).isEqualTo("tab_alias.col");
	}
}
