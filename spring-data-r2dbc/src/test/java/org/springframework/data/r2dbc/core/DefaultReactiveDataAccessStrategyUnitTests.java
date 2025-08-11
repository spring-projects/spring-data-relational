package org.springframework.data.r2dbc.core;

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.H2Dialect;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link DefaultReactiveDataAccessStrategy}.
 *
 * @author Jens Schauder
 */
class DefaultReactiveDataAccessStrategyUnitTests {

	DefaultReactiveDataAccessStrategy dataAccessStrategy = new DefaultReactiveDataAccessStrategy(H2Dialect.INSTANCE);

	@Test
	void getAllColumns() {

		SoftAssertions.assertSoftly(softly -> {
			check(softly, SimpleEntity.class, "ID", "NAME");
			check(softly, WithEmbedded.class, "ID", "L1_NAME", "L1_L2_NAME", "L1_L2_NUMBER");
			check(softly, WithEmbeddedId.class, "ID_NAME", "ID_NUMBER", "NAME");
		});
	}

	private void check(SoftAssertions softly, Class<?> entityType, String... columnNames) {

		List<SqlIdentifier> sqlIdentifiers = Arrays.stream(columnNames).map(SqlIdentifier::quoted).toList();
		softly.assertThat(dataAccessStrategy.getAllColumns(entityType)).describedAs(entityType.getName())
				.containsExactlyInAnyOrder(sqlIdentifiers.toArray(new SqlIdentifier[0]));
	}

	record SimpleEntity(int id, String name) {
	}

	record WithEmbedded(int id, @Embedded.Empty(prefix = "L1_") Level1 level1) {
	}

	record Level1(String name, @Embedded.Empty(prefix = "L2_") Level2 l2) {
	}

	record Level2(String name, Integer number) {
	}

	record WithEmbeddedId(@Id @Embedded.Empty(prefix = "ID_") Level2 id, String name) {
	}

}
