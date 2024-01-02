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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveSelectOperationExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveSelectOperationExtensionsUnitTests {

	val operations = mockk<FluentR2dbcOperations>(relaxed = true)

	val operationWithProjection = mockk<ReactiveSelectOperation.SelectWithProjection<Person>>(relaxed = true)

	@Test // gh-290
	fun `query() with reified type parameter extension should call its Java counterpart`() {

		operations.select<Person>()
		verify { operations.select(Person::class.java) }
	}

	@Test // gh-290
	fun `asType() with reified type parameter extension should call its Java counterpart`() {

		operationWithProjection.asType<User>();
		verify { operationWithProjection.`as`(User::class.java) }
	}

	@Test // gh-290
	fun terminatingFindAwaitOneWithValue() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.one() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitOne()).isEqualTo("foo")
		}

		verify {
			find.one()
		}
	}

	@Test // gh-290
	fun terminatingFindAwaitOneWithNull() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.one() } returns Mono.empty()

		assertThatExceptionOfType(NoSuchElementException::class.java).isThrownBy {
			runBlocking { find.awaitOne() }
		}

		verify {
			find.one()
		}
	}

	@Test // gh-290gh-290
	fun terminatingFindAwaitOneOrNullWithValue() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.one() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitOneOrNull()).isEqualTo("foo")
		}

		verify {
			find.one()
		}
	}

	@Test // gh-290gh-290
	fun terminatingFindAwaitOneOrNullWithNull() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.one() } returns Mono.empty()

		runBlocking {
			assertThat(find.awaitOneOrNull()).isNull()
		}

		verify {
			find.one()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitFirstWithValue() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.first() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitFirst()).isEqualTo("foo")
		}

		verify {
			find.first()
		}
	}

	@Test // gh-290gh-290
	fun terminatingFindAwaitFirstWithNull() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.first() } returns Mono.empty()

		assertThatExceptionOfType(NoSuchElementException::class.java).isThrownBy {
			runBlocking { find.awaitFirst() }
		}

		verify {
			find.first()
		}
	}

	@Test // gh-290gh-290
	fun terminatingFindAwaitFirstOrNullWithValue() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.first() } returns Mono.just("foo")

		runBlocking {
			assertThat(find.awaitFirstOrNull()).isEqualTo("foo")
		}

		verify {
			find.first()
		}
	}

	@Test // gh-290gh-290
	fun terminatingFindAwaitFirstOrNullWithNull() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.first() } returns Mono.empty()

		runBlocking {
			assertThat(find.awaitFirstOrNull()).isNull()
		}

		verify {
			find.first()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitCount() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.count() } returns Mono.just(1)

		runBlocking {
			assertThat(find.awaitCount()).isEqualTo(1)
		}

		verify {
			find.count()
		}
	}

	@Test // DATACASS-632
	fun terminatingFindAwaitExists() {

		val find = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { find.exists() } returns Mono.just(true)

		runBlocking {
			assertThat(find.awaitExists()).isTrue()
		}

		verify {
			find.exists()
		}
	}

	@Test // DATACASS-648
	fun terminatingFindAllAsFlow() {

		val spec = mockk<ReactiveSelectOperation.TerminatingSelect<String>>()
		every { spec.all() } returns Flux.just("foo", "bar", "baz")

		runBlocking {
			assertThat(spec.flow().toList()).contains("foo", "bar", "baz")
		}

		verify {
			spec.all()
		}
	}

	data class User(val id: String)
}
