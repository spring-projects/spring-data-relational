/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.core

import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.data.relational.core.query.Query

import java.util.Optional

/**
 * Extensions for [JdbcAggregateOperations].
 *
 * @author Felix Desyatirikov
 * @since 3.5
 */

/**
 * Extension for [JdbcAggregateOperations.count].
 */
inline fun <reified T> JdbcAggregateOperations.count(): Long =
		count(T::class.java)

/**
 * Extension for [JdbcAggregateOperations.count] with a query.
 */
inline fun <reified T> JdbcAggregateOperations.count(query: Query): Long =
		count(query, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.exists].
 */
inline fun <reified T> JdbcAggregateOperations.exists(query: Query): Boolean =
		exists(query, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.existsById].
 */
inline fun <reified T> JdbcAggregateOperations.existsById(id: Any): Boolean =
		existsById(id, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findById].
 */
inline fun <reified T> JdbcAggregateOperations.findById(id: Any): T? =
		findById(id, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findAllById].
 */
inline fun <reified T> JdbcAggregateOperations.findAllById(ids: Iterable<*>): List<T> =
		findAllById(ids, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findAll].
 */
inline fun <reified T> JdbcAggregateOperations.findAll(): List<T> =
		findAll(T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findAll] with sorting.
 */
inline fun <reified T> JdbcAggregateOperations.findAll(sort: Sort): List<T> =
		findAll(T::class.java, sort)

/**
 * Extension for [JdbcAggregateOperations.findAll] with pagination.
 */
inline fun <reified T> JdbcAggregateOperations.findAll(pageable: Pageable): Page<T> =
		findAll(T::class.java, pageable)

/**
 * Extension for [JdbcAggregateOperations.findOne] with a query.
 */
inline fun <reified T> JdbcAggregateOperations.findOne(query: Query): Optional<T> =
		findOne(query, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findAll] with a query.
 */
inline fun <reified T> JdbcAggregateOperations.findAll(query: Query): List<T> =
		findAll(query, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.findAll] with query and pagination.
 */
inline fun <reified T> JdbcAggregateOperations.findAll(query: Query, pageable: Pageable): Page<T> =
		findAll(query, T::class.java, pageable)

/**
 * Extension for [JdbcAggregateOperations.deleteById].
 */
inline fun <reified T> JdbcAggregateOperations.deleteById(id: Any): Unit =
		deleteById(id, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.deleteAllById].
 */
inline fun <reified T> JdbcAggregateOperations.deleteAllById(ids: Iterable<*>): Unit =
		deleteAllById(ids, T::class.java)

/**
 * Extension for [JdbcAggregateOperations.deleteAll].
 */
inline fun <reified T> JdbcAggregateOperations.deleteAll(): Unit =
		deleteAll(T::class.java)