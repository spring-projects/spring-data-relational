package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

	@ParameterizedTest
	@MethodSource("fixtures")
	void shouldReportAllColumns(Fixture fixture) {

		List<SqlIdentifier> sqlIdentifiers = Arrays.stream(fixture.allColumns()).map(SqlIdentifier::quoted).toList();

		assertThat(dataAccessStrategy.getAllColumns(fixture.entityType()))
				.containsExactlyInAnyOrder(sqlIdentifiers.toArray(new SqlIdentifier[0]));
	}

	static Stream<Fixture> fixtures() {
		return Stream.of(new Fixture(SimpleEntity.class, "ID", "NAME"),
				new Fixture(WithEmbedded.class, "ID", "L1_NAME", "L1_L2_NAME", "L1_L2_NUMBER"),
				new Fixture(WithEmbeddedId.class, "ID_NAME", "ID_NUMBER", "NAME"));
	}

	record Fixture(Class<?> entityType, String... allColumns) {

		@Override
		public String toString() {
			return entityType.getSimpleName();
		}
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
