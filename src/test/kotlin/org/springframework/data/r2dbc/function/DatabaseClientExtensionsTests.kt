/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.function

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import reactor.core.publisher.Mono

/**
 * Unit tests for [DatabaseClient] extensions.
 *
 * @author Sebastien Deleuze
 */
class DatabaseClientExtensionsTests {

	@Test // gh-63
	fun genericExecuteSpecAwait() {

		val spec = mockk<DatabaseClient.GenericExecuteSpec>()
		every { spec.then() } returns Mono.empty()

		runBlocking {
			spec.await()
		}

		verify {
			spec.then()
		}
	}

	@Test // gh-63
	fun genericExecuteSpecAsType() {

		val genericSpec = mockk<DatabaseClient.GenericExecuteSpec>()
		val typedSpec: DatabaseClient.TypedExecuteSpec<String> = mockk()
		every { genericSpec.`as`(String::class.java) } returns typedSpec

		runBlocking {
			assertThat(genericSpec.asType<String>()).isEqualTo(typedSpec)
		}

		verify {
			genericSpec.`as`(String::class.java)
		}
	}

	@Test // gh-63
	fun genericSelectSpecAsType() {

		val genericSpec = mockk<DatabaseClient.GenericSelectSpec>()
		val typedSpec: DatabaseClient.TypedSelectSpec<String> = mockk()
		every { genericSpec.`as`(String::class.java) } returns typedSpec

		runBlocking {
			assertThat(genericSpec.asType<String>()).isEqualTo(typedSpec)
		}

		verify {
			genericSpec.`as`(String::class.java)
		}
	}

	@Test // gh-63
	fun typedExecuteSpecAwait() {

		val spec = mockk<DatabaseClient.TypedExecuteSpec<String>>()
		every { spec.then() } returns Mono.empty()

		runBlocking {
			spec.await()
		}

		verify {
			spec.then()
		}
	}

	@Test // gh-63
	fun typedExecuteSpecAsType() {

		val spec: DatabaseClient.TypedExecuteSpec<String> = mockk()
		every { spec.`as`(String::class.java) } returns spec

		runBlocking {
			assertThat(spec.asType()).isEqualTo(spec)
		}

		verify {
			spec.`as`(String::class.java)
		}
	}

	@Test // gh-63
	fun insertSpecAwait() {

		val spec = mockk<DatabaseClient.InsertSpec<String>>()
		every { spec.then() } returns Mono.empty()

		runBlocking {
			spec.await()
		}

		verify {
			spec.then()
		}
	}

	@Test // gh-63
	fun insertIntoSpecInto() {

		val spec = mockk<DatabaseClient.InsertIntoSpec>()
		val typedSpec: DatabaseClient.TypedInsertSpec<String> = mockk()
		every { spec.into(String::class.java) } returns typedSpec

		runBlocking {
			assertThat(spec.into<String>()).isEqualTo(typedSpec)
		}

		verify {
			spec.into(String::class.java)
		}
	}
}
