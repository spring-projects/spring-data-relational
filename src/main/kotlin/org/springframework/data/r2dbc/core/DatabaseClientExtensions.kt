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
package org.springframework.data.r2dbc.core

import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.r2dbc.mapping.SettableValue

/**
 * Coroutines variant of [DatabaseClient.GenericExecuteSpec.then].
 *
 * @author Sebastien Deleuze
 */
suspend fun DatabaseClient.GenericExecuteSpec.await() {
	then().awaitFirstOrNull()
}

/**
 * Extension for [DatabaseClient.BindSpec.bind] providing a variant leveraging reified type parameters
 *
 * @author Mark Paluch
 * @author Ibanga Enoobong Ime
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> DatabaseClient.TypedExecuteSpec<*>.bind(index: Int, value: T?) = bind(index, SettableValue.fromOrEmpty(value, T::class.java))

/**
 * Extension for [DatabaseClient.BindSpec.bind] providing a variant leveraging reified type parameters
 *
 * @author Mark Paluch
 * @author Ibanga Enoobong Ime
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> DatabaseClient.GenericExecuteSpec.bind(index: Int, value: T?) = bind(index, SettableValue.fromOrEmpty(value, T::class.java))

/**
 * Extension for [DatabaseClient.BindSpec.bind] providing a variant leveraging reified type parameters
 *
 * @author Mark Paluch
 * @author Ibanga Enoobong Ime
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> DatabaseClient.TypedExecuteSpec<*>.bind(name: String, value: T?) = bind(name, SettableValue.fromOrEmpty(value, T::class.java))

/**
 * Extension for [DatabaseClient.BindSpec.bind] providing a variant leveraging reified type parameters
 *
 * @author Mark Paluch
 * @author Ibanga Enoobong Ime
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> DatabaseClient.GenericExecuteSpec.bind(name: String, value: T?) = bind(name, SettableValue.fromOrEmpty(value, T::class.java))

/**
 * Extension for [DatabaseClient.GenericExecuteSpec. as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.GenericExecuteSpec.asType(): DatabaseClient.TypedExecuteSpec<T> =
		`as`(T::class.java)

/**
 * Extension for [DatabaseClient.GenericSelectSpec.as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.GenericSelectSpec.asType(): DatabaseClient.TypedSelectSpec<T> =
		`as`(T::class.java)

/**
 * Coroutines variant of [DatabaseClient.TypedExecuteSpec.then].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> DatabaseClient.TypedExecuteSpec<T>.await() {
	then().awaitFirstOrNull()
}

/**
 * Extension for [DatabaseClient.TypedExecuteSpec. as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.TypedExecuteSpec<T>.asType(): DatabaseClient.TypedExecuteSpec<T> =
		`as`(T::class.java)

/**
 * Coroutines variant of [DatabaseClient.InsertSpec.then].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> DatabaseClient.InsertSpec<T>.await() {
	then().awaitFirstOrNull()
}

/**
 * Extension for [DatabaseClient.InsertIntoSpec.into] providing a
 * `into<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.InsertIntoSpec.into(): DatabaseClient.TypedInsertSpec<T> =
		into(T::class.java)

/**
 * Extension for [DatabaseClient.GenericInsertSpec.value] providing a variant leveraging reified type parameters
 *
 * @author Mark Paluch
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
inline fun <reified T : Any> DatabaseClient.GenericInsertSpec<*>.value(name: String, value: T?) = value(name, SettableValue.fromOrEmpty(value, T::class.java))


/**
 * Extension for [DatabaseClient.SelectFromSpec.from] providing a
 * `from<Foo>()` variant.
 *
 * @author Jonas Bark
 */
inline fun <reified T : Any> DatabaseClient.SelectFromSpec.from(): DatabaseClient.TypedSelectSpec<T> =
		from(T::class.java)

/**
 * Extension for [DatabaseClient.UpdateTableSpec.table] providing a
 * `table<Foo>()` variant.
 *
 * @author Mark Paluch
 */
inline fun <reified T : Any> DatabaseClient.UpdateTableSpec.table(): DatabaseClient.TypedUpdateSpec<T> =
		table(T::class.java)

/**
 * Extension for [DatabaseClient.SelectFromSpec.from] providing a
 * `from<Foo>()` variant.
 *
 * @author Jonas Bark
 */
inline fun <reified T : Any> DatabaseClient.DeleteFromSpec.from(): DatabaseClient.TypedDeleteSpec<T> =
		from(T::class.java)
