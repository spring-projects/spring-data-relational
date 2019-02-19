/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * Coroutines variant of [RowsFetchSpec.one].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitOne(): T?
        = one().awaitFirstOrNull()

/**
 * Coroutines variant of [RowsFetchSpec.first].
 *
 * @author Sebastien Deleuze
 */
suspend fun <T> RowsFetchSpec<T>.awaitFirst(): T?
        = first().awaitFirstOrNull()

// TODO Coroutines variant of [RowsFetchSpec.all], depends on [kotlinx.coroutines#254](https://github.com/Kotlin/kotlinx.coroutines/issues/254).
// suspend fun <T> RowsFetchSpec<T>.awaitAll() = all()...
