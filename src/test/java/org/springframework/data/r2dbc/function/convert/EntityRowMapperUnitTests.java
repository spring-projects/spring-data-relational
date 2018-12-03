package org.springframework.data.r2dbc.function.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

/**
 * Unit tests for {@link EntityRowMapper}.
 *
 * @author Mark Paluch
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

	@SuppressWarnings("unchecked")
	private <T> EntityRowMapper<T> getRowMapper(Class<T> type) {
		RelationalPersistentEntity<T> entity = (RelationalPersistentEntity<T>) strategy.getMappingContext()
				.getRequiredPersistentEntity(type);
		return new EntityRowMapper<>(entity, strategy.getRelationalConverter());
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
	}
}
