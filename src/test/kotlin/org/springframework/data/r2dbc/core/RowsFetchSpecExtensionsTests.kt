/*
 * Copyright 2018-2021 the original author or authors.
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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.Test
import org.springframework.dao.EmptyResultDataAccessException
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Unit tests for [RowsFetchSpec] extensions.
 *
 * @author Sebastien Deleuze
 * @author Mark Paluch
 */
class RowsFetchSpecExtensionsTests {

	@Test // gh-63
	fun awaitOneWithValue() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.one() } returns Mono.just("foo")

		runBlocking {
			assertThat(spec.awaitOne()).isEqualTo("foo")
		}

		verify {
			spec.one()
		}
	}

	@Test // gh-63, gh-103
	fun awaitOneWithNull() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.one() } returns Mono.empty()

		assertThatExceptionOfType(EmptyResultDataAccessException::class.java).isThrownBy {
			runBlocking { spec.awaitOne() }
		}

		verify {
			spec.one()
		}
	}

	@Test // gh-63
	fun awaitOneOrNullWithValue() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.one() } returns Mono.just("foo")

		runBlocking {
			assertThat(spec.awaitOneOrNull()).isEqualTo("foo")
		}

		verify {
			spec.one()
		}
	}

	@Test // gh-63
	fun awaitOneOrNullWithNull() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.one() } returns Mono.empty()

		runBlocking {
			assertThat(spec.awaitOneOrNull()).isNull()
		}

		verify {
			spec.one()
		}
	}

	@Test // gh-63
	fun awaitFirstWithValue() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.first() } returns Mono.just("foo")

		runBlocking {
			assertThat(spec.awaitFirst()).isEqualTo("foo")
		}

		verify {
			spec.first()
		}
	}

	@Test // gh-63, gh-103
	fun awaitFirstWithNull() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.first() } returns Mono.empty()

		assertThatExceptionOfType(EmptyResultDataAccessException::class.java).isThrownBy {
			runBlocking { spec.awaitFirst() }
		}

		verify {
			spec.first()
		}
	}

	@Test // gh-63
	fun awaitFirstOrNullWithValue() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.first() } returns Mono.just("foo")

		runBlocking {
			assertThat(spec.awaitFirstOrNull()).isEqualTo("foo")
		}

		verify {
			spec.first()
		}
	}

	@Test // gh-63
	fun awaitFirstOrNullWithNull() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.first() } returns Mono.empty()

		runBlocking {
			assertThat(spec.awaitFirstOrNull()).isNull()
		}

		verify {
			spec.first()
		}
	}

	@Test // gh-91
	@ExperimentalCoroutinesApi
	fun allAsFlow() {

		val spec = mockk<RowsFetchSpec<String>>()
		every { spec.all() } returns Flux.just("foo", "bar", "baz")

		runBlocking {
			assertThat(spec.flow().toList()).contains("foo", "bar", "baz")
		}

		verify {
			spec.all()
		}
	}
}
