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

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

/**
 * Unit tests for [Criteria.CriteriaStep] extensions.
 *
 * @author Juan Medina
 */
class CriteriaStepExtensionsTests {

    @Test // DATAJDBC-522
    fun eqIsCriteriaStep(){

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.`is`("test") } returns criteria

        assertThat(spec isEqual "test").isEqualTo(criteria)

        verify {
            spec.`is`("test")
        }
    }

    @Test // gh-1491
    fun notEqIsCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.not("test") } returns criteria

        assertThat(spec isNotEqual "test").isEqualTo(criteria)

        verify {
            spec.not("test")
        }
    }

    @Test // DATAJDBC-522
    fun inVarargCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.`in`(any() as Array<Any>) } returns criteria

        assertThat(spec.isIn("test")).isEqualTo(criteria)

        verify {
            spec.`in`(arrayOf("test"))
        }
    }

    @Test // DATAJDBC-522
    fun inListCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.`in`(listOf("test")) } returns criteria

        assertThat(spec.isIn(listOf("test"))).isEqualTo(criteria)

        verify {
            spec.`in`(listOf("test"))
        }
    }

    @Test // gh-1491
    fun leCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.lessThan(any()) } returns criteria

        assertThat(spec le 10).isEqualTo(criteria)

        verify {
            spec.lessThan(10)
        }
    }

    @Test // gh-1491
    fun loeCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.lessThanOrEquals(10) } returns criteria

        assertThat(spec loe 10).isEqualTo(criteria)

        verify {
            spec.lessThanOrEquals(10)
        }
    }

    @Test
    fun geCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.greaterThan(10) } returns criteria

        assertThat(spec ge 10).isEqualTo(criteria)

        verify {
            spec.greaterThan(10)
        }
    }

    @Test
    fun goeCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.greaterThanOrEquals(10) } returns criteria

        assertThat(spec goe 10).isEqualTo(criteria)

        verify {
            spec.greaterThanOrEquals(10)
        }
    }

    @Test
    fun likeCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.like("abc%") } returns criteria

        assertThat(spec like "abc%").isEqualTo(criteria)

        verify {
            spec.like("abc%")
        }
    }

    @Test
    fun notLikeCriteriaStep() {

        val spec = mockk<Criteria.CriteriaStep>()
        val criteria = mockk<Criteria>()

        every { spec.notLike("abc%") } returns criteria

        assertThat(spec notLike "abc%").isEqualTo(criteria)

        verify {
            spec.notLike("abc%")
        }
    }
}
