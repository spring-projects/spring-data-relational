/*
 * Copyright 2020-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository

import io.r2dbc.spi.R2dbcType
import io.r2dbc.spi.test.MockColumnMetadata
import io.r2dbc.spi.test.MockResult
import io.r2dbc.spi.test.MockRow
import io.r2dbc.spi.test.MockRowMetadata
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.dialect.PostgresDialect
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory
import org.springframework.data.r2dbc.testing.StatementRecorder
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.r2dbc.core.DatabaseClient

/**
 * Unit tests for [CoroutineCrudRepository].
 *
 * @author Mark Paluch
 */
class CoroutineRepositoryUnitTests {

	lateinit var client: DatabaseClient
	lateinit var entityTemplate: R2dbcEntityTemplate
	lateinit var recorder: StatementRecorder
	lateinit var repositoryFactory: R2dbcRepositoryFactory

	@BeforeEach
	fun before() {
		recorder = StatementRecorder.newInstance()
		client = DatabaseClient.builder().connectionFactory(recorder)
			.bindMarkers(PostgresDialect.INSTANCE.bindMarkersFactory).build()
		entityTemplate = R2dbcEntityTemplate(
			client,
			DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)
		)
		repositoryFactory = R2dbcRepositoryFactory(entityTemplate)
	}

	@Test
	fun shouldIssueDeleteQuery() {

		val result = MockResult.builder().rowsUpdated(2).build()
		recorder.addStubbing({ s: String -> s.startsWith("DELETE") }, result)

		val repository = repositoryFactory.getRepository(PersonRepository::class.java)

		runBlocking {
			repository.deleteUserAssociation(2)
		}
	}

	@Test // gh-395
	fun shouldIssueSelectQuery() {

		val rowMetadata = MockRowMetadata.builder()
			.columnMetadata(
				MockColumnMetadata.builder().name("id").type(R2dbcType.INTEGER).build()
			)
			.columnMetadata(
				MockColumnMetadata.builder().name("name").type(R2dbcType.VARCHAR).build()
			)
			.build()
		val row1 = MockRow.builder().identified("id", Object::class.java, 1L)
			.identified("name", Object::class.java, "Walter").metadata(rowMetadata)
			.build()
		val row2 = MockRow.builder().identified("id", Object::class.java, 2L)
			.identified("name", Object::class.java, "White").metadata(rowMetadata).build()

		val result = MockResult.builder().row(row1).row(row2).build()
		recorder.addStubbing({ s: String -> s.startsWith("SELECT") }, result)

		val repository = repositoryFactory.getRepository(PersonRepository::class.java)

		runBlocking {
			assertThat(repository.findAllByName("Walt")).hasSize(2)
		}
	}

	interface PersonRepository : CoroutineCrudRepository<Person, Long> {

		@Modifying
		@Query("DELETE FROM person WHERE id = :id ")
		suspend fun deleteUserAssociation(userId: Int)

		suspend fun findAllByName(name: String): List<Person>
	}

	data class Person(@Id var id: Long, var name: String)
}
