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

import kotlinx.coroutines.reactive.awaitFirstOrNull

/**
 * Coroutines variant of [DatabaseClient.GenericExecuteSpec.then].
 *
 * @author Sebastien Deleuze
 */
suspend fun DatabaseClient.GenericExecuteSpec.await() {
    then().awaitFirstOrNull()
}

/**
 * Extension for [DatabaseClient.GenericExecuteSpec.as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.GenericExecuteSpec.asType(): DatabaseClient.TypedExecuteSpec<T>
        = `as`(T::class.java)

/**
 * Extension for [DatabaseClient.GenericSelectSpec.as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.GenericSelectSpec.asType(): DatabaseClient.TypedSelectSpec<T>
        = `as`(T::class.java)

/**
 * Coroutines variant of [DatabaseClient.TypedExecuteSpec.then].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> DatabaseClient.TypedExecuteSpec<T>.await() {
    then().awaitFirstOrNull()
}

/**
 * Extension for [DatabaseClient.TypedExecuteSpec.as] providing a
 * `asType<Foo>()` variant.
 *
 * @author Sebastien Deleuze
 */
inline fun <reified T : Any> DatabaseClient.TypedExecuteSpec<T>.asType(): DatabaseClient.TypedExecuteSpec<T>
        = `as`(T::class.java)

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
inline fun <reified T : Any> DatabaseClient.InsertIntoSpec.into(): DatabaseClient.TypedInsertSpec<T>
        = into(T::class.java)

