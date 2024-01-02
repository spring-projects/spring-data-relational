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
package org.springframework.data.jdbc.core.dialect;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGobject;

/**
 * Unit tests for {@link JdbcPostgresDialect}.
 *
 * @author Jens Schauder
 */
public class JdbcPostgresDialectUnitTests {

	@Test // GH-1065
	void pgobjectIsConsideredSimple() {
		assertThat(JdbcPostgresDialect.INSTANCE.simpleTypes()).contains(PGobject.class);
	}

	@Test // GH-1065
	void geometricalTypesAreConsideredSimple() {

		assertThat(JdbcPostgresDialect.INSTANCE.simpleTypes()).contains( //
				PGpoint.class, //
				PGbox.class, //
				PGcircle.class, //
				org.postgresql.geometric.PGline.class, //
				PGpath.class, //
				PGpolygon.class, //
				PGlseg.class);
	}
}
