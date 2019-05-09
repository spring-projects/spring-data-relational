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

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.flow.asFlow

/**
 * Non-nullable Coroutines variant of [RowsFetchSpec.one].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitOne(): T =
		one().awaitSingle()

/**
 * Nullable Coroutines variant of [RowsFetchSpec.one].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitOneOrNull(): T? =
		one().awaitFirstOrNull()

/**
 * Non-nullable Coroutines variant of [RowsFetchSpec.first].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitFirst(): T =
		first().awaitSingle()

/**
 * Nullable Coroutines variant of [RowsFetchSpec.first].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitFirstOrNull(): T? =
		first().awaitFirstOrNull()

/**
 * Coroutines [Flow] variant of [RowsFetchSpec.all].
 *
 * Backpressure is controlled by [batchSize] parameter that controls the size of in-flight elements
 * and [org.reactivestreams.Subscription.request] size.
 *
 * @author Sebastien Deleuze
 */
@FlowPreview
fun <T: Any> RowsFetchSpec<T>.flow(batchSize: Int = 1): Flow<T> = all().asFlow(batchSize)
