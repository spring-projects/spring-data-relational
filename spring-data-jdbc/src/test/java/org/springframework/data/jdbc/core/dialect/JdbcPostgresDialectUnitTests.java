/*
 * Copyright 2021-2025 the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;

/**
 * Unit tests for {@link JdbcPostgresDialect}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class JdbcPostgresDialectUnitTests {

	@Test // GH-1065
	void pgobjectIsConsideredSimple() {
		assertThat(JdbcPostgresDialect.INSTANCE.simpleTypes()).contains(PGobject.class);
	}

	@ParameterizedTest // GH-1065, GH-2147
	@MethodSource("simpleTypes")
	void simpleTypesAreConsideredSimple(Class<?> type) {

		JdbcCustomConversions conversions = JdbcCustomConversions.of(JdbcPostgresDialect.INSTANCE, List.of());

		assertThat(conversions.isSimpleType(type)).isTrue();
		assertThat(conversions.getSimpleTypeHolder().isSimpleType(type)).isTrue();
	}

	static List<Class<?>> simpleTypes() {
		return List.of(PGpoint.class, //
				PGbox.class, //
				PGcircle.class, //
				org.postgresql.geometric.PGline.class, //
				PGpath.class, //
				PGpolygon.class, //
				PGlseg.class, //
				PGobject.class);
	}
}
