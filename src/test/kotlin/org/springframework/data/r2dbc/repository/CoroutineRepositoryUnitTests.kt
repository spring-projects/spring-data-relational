/*
 * Copyright 2020 the original author or authors.
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

import io.r2dbc.spi.test.MockResult
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.annotation.Id
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.dialect.PostgresDialect
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory
import org.springframework.data.r2dbc.testing.StatementRecorder
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

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
				.dataAccessStrategy(DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build()
		entityTemplate = R2dbcEntityTemplate(client)
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

	interface PersonRepository : CoroutineCrudRepository<Person, Long> {

		@Modifying
		@Query("DELETE FROM person WHERE id = :id ")
		suspend fun deleteUserAssociation(userId: Int)
	}


	data class Person(@Id var id: Long, var name: String)
}
