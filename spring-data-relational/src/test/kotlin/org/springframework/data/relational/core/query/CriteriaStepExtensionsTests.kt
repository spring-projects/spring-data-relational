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
}
