package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;

import java.util.List;

import org.junit.Test;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;

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
	public void shouldConsiderSimpleTypes() {

		SimpleTypeHolder holder = PostgresDialect.INSTANCE.getSimpleTypeHolder();

		assertSoftly(it -> {
			it.assertThat(holder.isSimpleType(String.class)).isTrue();
			it.assertThat(holder.isSimpleType(int.class)).isTrue();
			it.assertThat(holder.isSimpleType(Integer.class)).isTrue();
		});
	}

	@Test // gh-30
	public void shouldSupportArrays() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isTrue();
	}

	@Test // gh-30
	public void shouldUseBoxedArrayTypesForPrimitiveTypes() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertSoftly(it -> {
			it.assertThat(arrayColumns.getArrayType(int.class)).isEqualTo(Integer.class);
			it.assertThat(arrayColumns.getArrayType(double.class)).isEqualTo(Double.class);
			it.assertThat(arrayColumns.getArrayType(String.class)).isEqualTo(String.class);
		});
	}

	@Test // gh-30
	public void shouldRejectNonSimpleArrayTypes() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThatThrownBy(() -> arrayColumns.getArrayType(getClass())).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // gh-30
	public void shouldRejectNestedCollections() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThatThrownBy(() -> arrayColumns.getArrayType(List.class)).isInstanceOf(IllegalArgumentException.class);
	}
}
