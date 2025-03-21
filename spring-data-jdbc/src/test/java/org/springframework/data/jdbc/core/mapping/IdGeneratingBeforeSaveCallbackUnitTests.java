package org.springframework.data.jdbc.core.mapping;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.conversion.MappingRelationalConverter;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.MySqlDialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Sequence;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link IdGeneratingBeforeSaveCallback}
 *
 * @author Mikhail Polivakha
 */
class IdGeneratingBeforeSaveCallbackUnitTests {

	@Test // GH-1923
	void mySqlDialectSequenceGenerationIsNotSupported() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
		MySqlDialect mySqlDialect = new MySqlDialect(IdentifierProcessing.NONE);
		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);
		RelationalConverter converter = new MappingRelationalConverter(relationalMappingContext);

		IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(mySqlDialect,
				operations, converter);

		NoSequenceEntity entity = new NoSequenceEntity();

		Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

		Assertions.assertThat(processed).isSameAs(entity);
		Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
	}

	@Test // GH-1923
	void entityIsNotMarkedWithTargetSequence() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
		RelationalConverter converter = new MappingRelationalConverter(relationalMappingContext);
		PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

		IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(mySqlDialect,
				operations, converter);

		NoSequenceEntity entity = new NoSequenceEntity();

		Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

		Assertions.assertThat(processed).isSameAs(entity);
		Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
	}

	@Test // GH-1923
	void entityIdIsPopulatedFromSequence() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
		RelationalConverter converter = new MappingRelationalConverter(relationalMappingContext);
		relationalMappingContext.getRequiredPersistentEntity(EntityWithSequence.class);

		PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

		long generatedId = 112L;
		when(operations.queryForObject(anyString(), anyMap(), any(Class.class))).thenReturn(generatedId);

		IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(mySqlDialect,
				operations, converter);

		EntityWithSequence entity = new EntityWithSequence();

		Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

		Assertions.assertThat(processed).isSameAs(entity);
		Assertions.assertThat(processed).usingRecursiveComparison().ignoringFields("id").isEqualTo(entity);
		Assertions.assertThat(entity.getId()).isEqualTo(generatedId);
	}

	@Test // GH-1923
	void entityWithSequenceEmbeddedIdIsPopulatedFromSequence() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
		RelationalConverter converter = new MappingRelationalConverter(relationalMappingContext);
		relationalMappingContext.getRequiredPersistentEntity(EntityWithSequenceOnEmbeddedId.class);

		PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

		long generatedId = 112L;
		when(operations.queryForObject(anyString(), anyMap(), any(Class.class))).thenReturn(generatedId);

		IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(mySqlDialect,
				operations, converter);

		EntityWithSequenceOnEmbeddedId entity = new EntityWithSequenceOnEmbeddedId(null, "test name");

		EntityWithSequenceOnEmbeddedId processed = (EntityWithSequenceOnEmbeddedId) subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

		Assertions.assertThat(processed).usingRecursiveComparison().ignoringFields("id").isEqualTo(entity);
		Assertions.assertThat(processed.id).isEqualTo(new EmbeddedId(generatedId));
	}

	@Table
	static class NoSequenceEntity {

		@Id private Long id;
		private Long name;
	}

	@Table
	static class EntityWithSequence {

		@Id
		@Sequence(value = "id_seq", schema = "public") private Long id;

		private Long name;

		public Long getId() {
			return id;
		}
	}

	@Table
	record EntityWithSequenceOnEmbeddedId(
			@Id @Embedded.Nullable @Sequence(value = "id_seq", schema = "public") EmbeddedId id, String name) {
	}

	record EmbeddedId(Long id) {
	}

}
