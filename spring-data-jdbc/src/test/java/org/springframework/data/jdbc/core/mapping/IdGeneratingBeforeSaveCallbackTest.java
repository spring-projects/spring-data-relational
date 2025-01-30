package org.springframework.data.jdbc.core.mapping;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.dialect.MySqlDialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Sequence;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link IdGeneratingBeforeSaveCallback}
 *
 * @author Mikhail Polivakha
 */
class IdGeneratingBeforeSaveCallbackTest {

    @Test // GH-1923
    void mySqlDialectsequenceGenerationIsNotSupported() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        MySqlDialect mySqlDialect = new MySqlDialect(IdentifierProcessing.NONE);
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        NoSequenceEntity entity = new NoSequenceEntity();

        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        Assertions.assertThat(processed).isSameAs(entity);
        Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
    }

    @Test // GH-1923
    void entityIsNotMarkedWithTargetSequence() {

		RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        NoSequenceEntity entity = new NoSequenceEntity();

        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        Assertions.assertThat(processed).isSameAs(entity);
        Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
    }

    @Test // GH-1923
    void entityIdIsPopulatedFromSequence() {

        RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        relationalMappingContext.getRequiredPersistentEntity(EntityWithSequence.class);

        PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        long generatedId = 112L;
        when(operations.queryForObject(anyString(), anyMap(), any(RowMapper.class))).thenReturn(generatedId);

        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        EntityWithSequence entity = new EntityWithSequence();

        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        Assertions.assertThat(processed).isSameAs(entity);
        Assertions
          .assertThat(processed)
          .usingRecursiveComparison()
          .ignoringFields("id")
          .isEqualTo(entity);
        Assertions.assertThat(entity.getId()).isEqualTo(generatedId);
    }

    @Table
    static class NoSequenceEntity {

        @Id
        private Long id;
        private Long name;
    }

    @Table
    static class EntityWithSequence {

        @Id
        @Sequence(value = "id_seq", schema = "public")
        private Long id;

        private Long name;

        public Long getId() {
            return id;
        }
    }
}