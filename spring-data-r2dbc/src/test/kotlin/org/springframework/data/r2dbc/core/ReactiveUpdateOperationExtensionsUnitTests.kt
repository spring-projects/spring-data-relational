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
import org.springframework.data.relational.core.query.Update
import reactor.core.publisher.Mono

/**
 * Unit tests for [ReactiveUpdateOperationExtensions].
 *
 * @author Mark Paluch
 */
class ReactiveUpdateOperationExtensionsUnitTests {

	val operations = mockk<FluentR2dbcOperations>(relaxed = true)

	@Test// gh-290
	fun `update() with reified type parameter extension should call its Java counterpart`() {

		operations.update<Person>()
		verify { operations.update(Person::class.java) }
	}

	@Test // gh-290
	fun applyAndAwait() {

		val update = mockk<ReactiveUpdateOperation.TerminatingUpdate>()
		val updateObj = mockk<Update>();
		every { update.apply(updateObj) } returns Mono.just(42)

		runBlocking {
			assertThat(update.applyAndAwait(updateObj)).isEqualTo(42)
		}

		verify {
			update.apply(updateObj)
		}
	}
}
