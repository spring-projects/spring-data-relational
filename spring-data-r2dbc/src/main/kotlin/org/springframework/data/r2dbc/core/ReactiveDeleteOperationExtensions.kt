/*
 * Copyright 2018-2024 the original author or authors.
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

import kotlinx.coroutines.reactive.awaitSingle

/**
 * Extensions for [ReactiveDeleteOperation].
 *
 * @author Mark Paluch
 * @since 1.1
 */

/**
 * Extension for [ReactiveDeleteOperation.delete] leveraging reified type parameters.
 */
inline fun <reified T : Any> ReactiveDeleteOperation.delete(): ReactiveDeleteOperation.ReactiveDelete =
		delete(T::class.java)

/**
 * Coroutines variant of [ReactiveDeleteOperation.TerminatingDelete.all].
 */
suspend fun ReactiveDeleteOperation.TerminatingDelete.allAndAwait(): Long =
		all().awaitSingle()
