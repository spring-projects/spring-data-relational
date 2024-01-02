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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull

/**
 * Extensions for [ReactiveSelectOperation].
 *
 * @author Mark Paluch
 * @author Oleg Oshmyan
 * @author George Papadopoulos
 * @since 1.1
 */

/**
 * Extension for [ReactiveSelectOperation.select] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveSelectOperation.select(): ReactiveSelectOperation.ReactiveSelect<T> =
		select(T::class.java)

/**
 * Extension for [ReactiveSelectOperation.SelectWithProjection. as] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveSelectOperation.SelectWithProjection<*>.asType(): ReactiveSelectOperation.SelectWithQuery<T> =
		`as`(T::class.java)

/**
 * Non-nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.one].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitOne(): T =
		one().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.one].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitOneOrNull(): T? =
		one().awaitSingleOrNull()

/**
 * Non-nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.first].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitFirst(): T =
		first().awaitSingle()

/**
 * Nullable Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.first].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitFirstOrNull(): T? =
		first().awaitSingleOrNull()

/**
 * Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.count].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitCount(): Long =
		count().awaitSingle()

/**
 * Coroutines variant of [ReactiveSelectOperation.TerminatingSelect.exists].
 */
suspend fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.awaitExists(): Boolean =
		exists().awaitSingle()

/**
 * Coroutines [Flow] variant of [ReactiveSelectOperation.TerminatingSelect.all].
 */
fun <T : Any> ReactiveSelectOperation.TerminatingSelect<T>.flow(): Flow<T> =
		all().asFlow()
