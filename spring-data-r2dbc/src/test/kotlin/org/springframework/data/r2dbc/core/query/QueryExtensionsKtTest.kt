package org.springframework.data.r2dbc.core.query

import io.r2dbc.spi.R2dbcType
import io.r2dbc.spi.test.MockColumnMetadata
import io.r2dbc.spi.test.MockResult
import io.r2dbc.spi.test.MockRow
import io.r2dbc.spi.test.MockRowMetadata
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.springframework.data.r2dbc.core.*
import org.springframework.data.r2dbc.dialect.PostgresDialect
import org.springframework.data.r2dbc.testing.StatementRecorder
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.isEqual
import org.springframework.r2dbc.core.DatabaseClient
import reactor.test.StepVerifier

/**
 * Unit tests for [QueryExtensions].
 *
 * @author kihwankim
 */
class QueryExtensionsKtTest {
    private lateinit var client: DatabaseClient
    private lateinit var entityTemplate: R2dbcEntityTemplate
    private lateinit var recorder: StatementRecorder

    @BeforeEach
    fun before() {
        recorder = StatementRecorder.newInstance()
        client = DatabaseClient.builder().connectionFactory(recorder)
            .bindMarkers(PostgresDialect.INSTANCE.bindMarkersFactory).build()
        entityTemplate = R2dbcEntityTemplate(client, DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE))
    }

    @ParameterizedTest // gh-1491
    @EnumSource(QueryConditionType::class)
    fun shouldSelectAll(queryCondition: QueryConditionType) {
        val metadata = MockRowMetadata.builder()
            .columnMetadata(MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build())
            .columnMetadata(MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build())
            .build()
        val result = queryCondition.nameValue?.let {
            listOf(
                MockResult.builder()
                    .row(
                        MockRow.builder()
                            .identified("id", Any::class.java, 1L)
                            .identified("name", Any::class.java, queryCondition.nameValue)
                            .metadata(metadata)
                            .build()
                    ).build()
            )
        } ?: emptyList()

        recorder.addStubbing({ s: String -> s.startsWith("SELECT") }, result)

        entityTemplate.select<Person>()
            .matching(
                query(
                    queryCondition.nameValue?.run { where("name") isEqual queryCondition.nameValue }
                )
            )
            .all()
            .`as`(StepVerifier::create)
            .expectNextCount(queryCondition.count)
            .verifyComplete()
        val statement = recorder.getCreatedStatement { s: String -> s.startsWith("SELECT") }
        Assertions.assertThat(statement.sql)
            .isEqualTo(queryCondition.resultSql)
    }

    enum class QueryConditionType(val nameValue: String?, val count: Long, val resultSql: String) {
        FILTER("testName", 1L, "SELECT person.* FROM person WHERE (person.name = $1)"),
        NON_FILTER(null, 0L, "SELECT person.* FROM person")
    }
}
