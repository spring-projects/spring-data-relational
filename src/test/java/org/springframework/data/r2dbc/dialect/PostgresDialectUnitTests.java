package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.springframework.data.mapping.model.SimpleTypeHolder;

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

	@Test // gh-30
	public void shouldConsiderCollectionTypesAsSimple() {

		SimpleTypeHolder holder = PostgresDialect.INSTANCE.getSimpleTypeHolder();

		assertThat(holder.isSimpleType(List.class)).isTrue();
		assertThat(holder.isSimpleType(Collection.class)).isTrue();
	}

	@Test // gh-30
	public void shouldConsiderStringArrayTypeAsSimple() {

		SimpleTypeHolder holder = PostgresDialect.INSTANCE.getSimpleTypeHolder();

		assertThat(holder.isSimpleType(String[].class)).isTrue();
	}
}
