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
package org.springframework.data.r2dbc.core

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveInsertOperationExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveInsertOperationExtensionsUnitTests {

	val operations = mockk<FluentR2dbcOperations>(relaxed = true)

	@Test // gh-290
	fun `insert() with reified type parameter extension should call its Java counterpart`() {

		operations.insert<Person>()
		verify { operations.insert(Person::class.java) }
	}

	@Test // gh-290
	fun oneAndAwait() {

		val insert = mockk<ReactiveInsertOperation.TerminatingInsert<String>>()
		every { insert.using("foo") } returns Mono.just("bar")

		runBlocking {
			assertThat(insert.usingAndAwait("foo")).isEqualTo("bar")
		}

		verify {
			insert.using("foo")
		}
	}
}
