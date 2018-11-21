package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for {@link PostgresDialect}.
 *
 * @author Mark Paluch
 */
public class PostgresDialectUnitTests {

	@Test // gh-20
	public void shouldUsePostgresPlaceholders() {

		BindMarkers bindMarkers = PostgresDialect.INSTANCE.getBindMarkersFactory().create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next("foo");

		assertThat(first.getPlaceholder()).isEqualTo("$1");
		assertThat(second.getPlaceholder()).isEqualTo("$2");
	}
}
