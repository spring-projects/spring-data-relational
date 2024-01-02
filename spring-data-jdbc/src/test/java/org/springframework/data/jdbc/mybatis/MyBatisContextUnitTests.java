/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.jdbc.mybatis;

import static org.assertj.core.api.SoftAssertions.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.relational.core.sql.SqlIdentifier;

public class MyBatisContextUnitTests {

	@Test // DATAJDBC-542
	public void testGetReturnsValuesFromIdentifier() {

		Map<SqlIdentifier, Object> map = new HashMap<>();
		map.put(SqlIdentifier.quoted("one"), "oneValue");
		map.put(SqlIdentifier.unquoted("two"), "twoValue");
		map.put(SqlIdentifier.quoted("Three"), "threeValue");
		map.put(SqlIdentifier.unquoted("Four"), "fourValue");

		MyBatisContext context = new MyBatisContext(Identifier.from(map), null, null);

		assertSoftly(softly -> {

			softly.assertThat(context.get("one")).isEqualTo("oneValue");
			softly.assertThat(context.get("two")).isEqualTo("twoValue");
			softly.assertThat(context.get("Three")).isEqualTo("threeValue");
			softly.assertThat(context.get("Four")).isEqualTo("fourValue");
			softly.assertThat(context.get("four")).isNull();
			softly.assertThat(context.get("five")).isNull();
		});
	}

}
