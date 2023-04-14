/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.relational.core.query

/**
 * Extension for [Criteria.CriteriaStep.is] providing a
 * `isEquals(value)` variant.
 *
 * @author Juan Medina
 * @since 2.1
 */
infix fun Criteria.CriteriaStep.isEqual(value: Any): Criteria =
        `is`(value)


/**
 * Extension for [Criteria.CriteriaStep.not] providing a
 * `isNotEqual(value)` variant.
 */
infix fun Criteria.CriteriaStep.isNotEqual(value: Any): Criteria =
        not(value)

/**
 * Extension for [Criteria.CriteriaStep.in] providing a
 * `isIn(value)` variant.
 *
 * @author Juan Medina
 * @since 2.1
 */
fun Criteria.CriteriaStep.isIn(vararg value: Any): Criteria =
        `in`(value)

/**
 * Extension for [Criteria.CriteriaStep.in] providing a
 * `isIn(value)` variant.
 *
 * @author Juan Medina
 * @since 2.1
 */
fun Criteria.CriteriaStep.isIn(values: Collection<Any>): Criteria =
        `in`(values)


/**
 * Extension for [Criteria.CriteriaStep.lessThan] providing a
 * `le(value)` variant.
 */
infix fun Criteria.CriteriaStep.le(value: Any): Criteria =
        lessThan(value)

/**
 * Extension for [Criteria.CriteriaStep.lessThanOrEquals] providing a
 * `loe(value)` variant.
 */
infix fun Criteria.CriteriaStep.loe(value: Any): Criteria =
        lessThanOrEquals(value)

/**
 * Extension for [Criteria.CriteriaStep.greaterThan] providing a
 * `ge(value)` variant.
 */
infix fun Criteria.CriteriaStep.ge(value: Any): Criteria =
        greaterThan(value)

/**
 * Extension for [Criteria.CriteriaStep.greaterThanOrEquals] providing a
 * `goe(value)` variant.
 */
infix fun Criteria.CriteriaStep.goe(value: Any): Criteria =
        greaterThanOrEquals(value)

/**
 * Extension for [Criteria.CriteriaStep.like] providing a
 * `like(value)` variant.
 */
infix fun Criteria.CriteriaStep.like(value: Any): Criteria =
        like(value)

/**
 * Extension for [Criteria.CriteriaStep.notLike] providing a
 * `notLike(value)` variant.
 */
infix fun Criteria.CriteriaStep.notLike(value: Any): Criteria =
        notLike(value)
