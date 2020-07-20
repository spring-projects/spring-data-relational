package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.junit.Test;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;

/**
 * Unit tests for {@link SqlServerDialect}.
 *
 * @author Mark Paluch
 */
public class SqlServerDialectUnitTests {

	@Test // gh-20
	public void shouldUseNamedPlaceholders() {

		BindMarkers bindMarkers = SqlServerDialect.INSTANCE.getBindMarkersFactory().create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next("'foo!bar");

		assertThat(first.getPlaceholder()).isEqualTo("@P0");
		assertThat(second.getPlaceholder()).isEqualTo("@P1_foobar");
	}

	@Test // gh-30
	public void shouldConsiderUuidAsSimple() {

		SimpleTypeHolder holder = SqlServerDialect.INSTANCE.getSimpleTypeHolder();

		assertThat(holder.isSimpleType(UUID.class)).isTrue();
	}

	@Test // gh-30
	public void shouldNotSupportArrays() {

		ArrayColumns arrayColumns = SqlServerDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isFalse();
		assertThatThrownBy(() -> arrayColumns.getArrayType(String.class)).isInstanceOf(UnsupportedOperationException.class);
	}
}
