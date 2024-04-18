/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.r2dbc

import io.r2dbc.spi.Row

/**
 * Extensions for [Row].
 *
 * @author George Papadopoulos
 */

/**
 * Returns the value at the specified [column][name] in the resulting [Row].
 * You can use [Any] as `T` to allow the implementation to make the loosest possible match.
 *
 * See [getColumnNullable] when column contains `nullable` values.
 *
 * @throws IllegalStateException when column contains a `null` value.
 * @throws NoSuchElementException if the [name] is not a know readable column.
 */
inline fun <reified T : Any> Row.getColumn(name: String): T =
    getColumnNullable(name) ?: error("There is a null value inside column:$name.")

/**
 * Returns the value at the specified [column][name] in the resulting [Row]. The value can be `null`.
 * You can use [Any] as `T` to allow the implementation to make the loosest possible match.
 *
 * @throws NoSuchElementException if the [name] is not a know readable column.
 */
inline fun <reified T : Any> Row.getColumnNullable(name: String): T? = this[name, T::class.java]

/**
 * Returns the value at the specified [column-index][index] in the resulting [Row].
 * You can use [Any] as `T` to allow the implementation to make the loosest possible match.
 *
 * See [getColumnNullable] when column contains `nullable` values.
 *
 * @throws IllegalStateException when column contains a `null` value.
 * @throws IndexOutOfBoundsException if the [index] is out of range (negative or equals/exceeds the number of readable objects).
 */
inline fun <reified T : Any> Row.getColumn(index: Int): T =
    getColumnNullable(index) ?: error("There is a null value inside column-index:$index.")

/**
 * Returns the value at the specified [column-index][index] in the resulting [Row], the value can be `null`.
 * You can use [Any] as `T` to allow the implementation to make the loosest possible match.
 *
 * @throws IndexOutOfBoundsException if the [index] is out of range (negative or equals/exceeds the number of readable objects).
 */
inline fun <reified T : Any> Row.getColumnNullable(index: Int): T? = this[index, T::class.java]