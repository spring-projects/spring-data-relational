/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.data.relational.repository.support;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Tests for {@link TableNameQueryPreprocessor}.
 *
 * @author Jens Schauder
 */
class TableNameQueryPreprocessorUnitTests {

	@Test // GH-1856
	void transform() {

		TableNameQueryPreprocessor preprocessor = new TableNameQueryPreprocessor(SqlIdentifier.quoted("some_table_name"), SqlIdentifier.quoted("qualified_table_name"), AnsiDialect.INSTANCE);
		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(preprocessor.transform("someString")).isEqualTo("someString");
			softly.assertThat(preprocessor.transform("someString#{#tableName}restOfString"))
					.isEqualTo("someString\"some_table_name\"restOfString");
			softly.assertThat(preprocessor.transform("select from #{#tableName} where x = :#{#some other spel}"))
					.isEqualTo("select from \"some_table_name\" where x = :#{#some other spel}");
			softly.assertThat(preprocessor.transform("select from #{#qualifiedTableName}"))
					.isEqualTo("select from \"qualified_table_name\"");
		});
	}
}
