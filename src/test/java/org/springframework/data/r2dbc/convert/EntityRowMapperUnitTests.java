package org.springframework.data.r2dbc.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.r2dbc.convert.EntityRowMapper;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.PostgresDialect;

/**
 * Unit tests for {@link EntityRowMapper}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityRowMapperUnitTests {

	DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE);

	Row rowMock = mock(Row.class);
	RowMetadata metadata = mock(RowMetadata.class);

	@Test // gh-22
	public void shouldMapSimpleEntity() {

		EntityRowMapper<SimpleEntity> mapper = getRowMapper(SimpleEntity.class);
		when(rowMock.get("id")).thenReturn("foo");

		SimpleEntity result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo("foo");
	}

	@Test // gh-22
	public void shouldMapSimpleEntityWithConstructorCreation() {

		EntityRowMapper<SimpleEntityConstructorCreation> mapper = getRowMapper(SimpleEntityConstructorCreation.class);
		when(rowMock.get("id")).thenReturn("foo");

		SimpleEntityConstructorCreation result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo("foo");
	}

	@Test // gh-22
	public void shouldApplyConversionWithConstructorCreation() {

		EntityRowMapper<ConversionWithConstructorCreation> mapper = getRowMapper(ConversionWithConstructorCreation.class);
		when(rowMock.get("id")).thenReturn((byte) 0x24);

		ConversionWithConstructorCreation result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo(36L);
	}

	@Test // gh-30
	public void shouldConvertArrayToCollection() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("ids")).thenReturn((new String[] { "foo", "bar" }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.ids).contains("foo", "bar");
	}

	@Test // gh-30
	public void shouldConvertArrayToSet() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("integer_set")).thenReturn((new int[] { 3, 14 }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.integerSet).contains(3, 14);
	}

	@Test // gh-30
	public void shouldConvertArrayMembers() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("primitive_integers")).thenReturn((new Long[] { 3L, 14L }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.primitiveIntegers).contains(3, 14);
	}

	@Test // gh-30
	public void shouldConvertArrayToBoxedArray() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("boxed_integers")).thenReturn((new int[] { 3, 11 }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.boxedIntegers).contains(3, 11);
	}

	private <T> EntityRowMapper<T> getRowMapper(Class<T> type) {
		return new EntityRowMapper<>(type, strategy.getConverter());
	}

	static class SimpleEntity {
		String id;
	}

	@RequiredArgsConstructor
	static class SimpleEntityConstructorCreation {
		final String id;
	}

	@RequiredArgsConstructor
	static class ConversionWithConstructorCreation {
		final long id;
	}

	static class EntityWithCollection {
		List<String> ids;
		Set<Integer> integerSet;
		Integer[] boxedIntegers;
		int[] primitiveIntegers;
	}
}
