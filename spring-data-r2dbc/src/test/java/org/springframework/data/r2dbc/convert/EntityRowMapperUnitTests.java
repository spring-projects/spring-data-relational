package org.springframework.data.r2dbc.convert;

import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockRowMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.PostgresDialect;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link EntityRowMapper}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
class EntityRowMapperUnitTests {

	private DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE);

	private Row rowMock = mock(Row.class);
	private RowMetadata metadata = MockRowMetadata.builder()
			.columnMetadata(MockColumnMetadata.builder().name("integer_set").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("boxed_integers").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("primitive_integers").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("enum_array").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("set_of_enum").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("enum_set").type(R2dbcType.COLLECTION).build())
			.columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
			.columnMetadata(MockColumnMetadata.builder().name("ids").type(R2dbcType.COLLECTION).build()).build();

	@Test // gh-22
	void shouldMapSimpleEntity() {

		EntityRowMapper<SimpleEntity> mapper = getRowMapper(SimpleEntity.class);
		when(rowMock.get("id")).thenReturn("foo");

		SimpleEntity result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo("foo");
	}

	@Test // gh-22
	void shouldMapSimpleEntityWithConstructorCreation() {

		EntityRowMapper<SimpleEntityConstructorCreation> mapper = getRowMapper(SimpleEntityConstructorCreation.class);
		when(rowMock.get("id")).thenReturn("foo");

		SimpleEntityConstructorCreation result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo("foo");
	}

	@Test // gh-22
	void shouldApplyConversionWithConstructorCreation() {

		EntityRowMapper<ConversionWithConstructorCreation> mapper = getRowMapper(ConversionWithConstructorCreation.class);
		when(rowMock.get("id")).thenReturn((byte) 0x24);

		ConversionWithConstructorCreation result = mapper.apply(rowMock, metadata);
		assertThat(result.id).isEqualTo(36L);
	}

	@Test // gh-30
	void shouldConvertArrayToCollection() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("ids")).thenReturn((new String[] { "foo", "bar" }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.ids).contains("foo", "bar");
	}

	@Test // gh-30
	void shouldConvertArrayToSet() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("integer_set")).thenReturn((new int[] { 3, 14 }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.integerSet).contains(3, 14);
	}

	@Test // gh-30
	void shouldConvertArrayMembers() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("primitive_integers")).thenReturn((new Long[] { 3L, 14L }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.primitiveIntegers).contains(3, 14);
	}

	@Test // gh-30
	void shouldConvertArrayToBoxedArray() {

		EntityRowMapper<EntityWithCollection> mapper = getRowMapper(EntityWithCollection.class);
		when(rowMock.get("boxed_integers")).thenReturn((new int[] { 3, 11 }));

		EntityWithCollection result = mapper.apply(rowMock, metadata);
		assertThat(result.boxedIntegers).contains(3, 11);
	}

	@Test // gh-252
	void shouldReadEnums() {

		EntityRowMapper<WithEnumCollections> mapper = getRowMapper(WithEnumCollections.class);
		when(rowMock.get("enum_array")).thenReturn((new String[] { "ONE", "TWO" }));
		when(rowMock.get("set_of_enum")).thenReturn((new String[] { "ONE", "THREE" }));
		when(rowMock.get("enum_set")).thenReturn((new String[] { "ONE", "TWO" }));

		WithEnumCollections result = mapper.apply(rowMock, metadata);
		assertThat(result.enumArray).contains(MyEnum.ONE, MyEnum.TWO);
		assertThat(result.setOfEnum).contains(MyEnum.ONE, MyEnum.THREE);
		assertThat(result.enumSet).contains(MyEnum.ONE, MyEnum.TWO);
	}

	private <T> EntityRowMapper<T> getRowMapper(Class<T> type) {
		return new EntityRowMapper<>(type, strategy.getConverter());
	}

	static class SimpleEntity {
		String id;
	}

	static class SimpleEntityConstructorCreation {
		final String id;

		public SimpleEntityConstructorCreation(String id) {
			this.id = id;
		}
	}

	static class ConversionWithConstructorCreation {
		final long id;

		public ConversionWithConstructorCreation(long id) {
			this.id = id;
		}
	}

	static class EntityWithCollection {
		List<String> ids;
		Set<Integer> integerSet;
		Integer[] boxedIntegers;
		int[] primitiveIntegers;
	}

	static class WithEnumCollections {

		MyEnum[] enumArray;
		Set<MyEnum> setOfEnum;
		EnumSet<MyEnum> enumSet;
	}

	enum MyEnum {
		ONE, TWO, THREE
	}

}
