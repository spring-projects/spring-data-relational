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
import org.springframework.data.relational.core.mapping.TargetSequence;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link IdGeneratingBeforeSaveCallback}
 *
 * @author Mikhail Polivakha
 */
class IdGeneratingBeforeSaveCallbackTest {

    @Test
    void test_mySqlDialect_sequenceGenerationIsNotSupported() {
        // given
        RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        MySqlDialect mySqlDialect = new MySqlDialect(IdentifierProcessing.NONE);
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        // and
        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        NoSequenceEntity entity = new NoSequenceEntity();

        // when
        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        // then
        Assertions.assertThat(processed).isSameAs(entity);
        Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
    }

    @Test
    void test_EntityIsNotMarkedWithTargetSequence() {
        // given
        RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        // and
        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        NoSequenceEntity entity = new NoSequenceEntity();

        // when
        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        // then
        Assertions.assertThat(processed).isSameAs(entity);
        Assertions.assertThat(processed).usingRecursiveComparison().isEqualTo(entity);
    }

    @Test
    void test_EntityIdIsPopulatedFromSequence() {
        // given
        RelationalMappingContext relationalMappingContext = new RelationalMappingContext();
        relationalMappingContext.getRequiredPersistentEntity(EntityWithSequence.class);

        PostgresDialect mySqlDialect = PostgresDialect.INSTANCE;
        NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

        // and
        long generatedId = 112L;
        when(operations.queryForObject(anyString(), anyMap(), any(RowMapper.class))).thenReturn(generatedId);

        // and
        IdGeneratingBeforeSaveCallback subject = new IdGeneratingBeforeSaveCallback(relationalMappingContext, mySqlDialect, operations);

        EntityWithSequence entity = new EntityWithSequence();

        // when
        Object processed = subject.onBeforeSave(entity, MutableAggregateChange.forSave(entity));

        // then
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
        @TargetSequence(value = "id_seq", schema = "public")
        private Long id;

        private Long name;

        public Long getId() {
            return id;
        }
    }
}