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

package org.springframework.data.jdbc.core

import io.mockk.mockk
import io.mockk.verify
import org.junit.Test
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.data.jdbc.testing.TestClass
import org.springframework.data.relational.core.query.Query

/**
 * Unit tests for [JdbcAggregateOperations].
 *
 * @author Felix Desyatirikov
 */

class JdbcAggregateOperationsExtensionsTests {

    val operations = mockk<JdbcAggregateOperations>(relaxed = true)

    @Test // GH-1961
    fun `count with reified type parameter extension should call its Java counterpart`() {

        operations.count<TestClass>()

        verify { operations.count(TestClass::class.java) }
    }

    @Test // GH-1961
    fun `count(Query) with reified type parameter extension should call its Java counterpart`() {

        val query = mockk<Query>(relaxed = true)

        operations.count<TestClass>(query)

        verify {
            operations.count(query, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `exists(Query) with reified type parameter extension should call its Java counterpart`() {

        val query = mockk<Query>(relaxed = true)

        operations.exists<TestClass>(query)

        verify {
            operations.exists(query, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `existsById(id) with reified type parameter extension should call its Java counterpart`() {

        val id = 1L

        operations.existsById<TestClass>(id)

        verify {
            operations.existsById(id, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `findById(id) with reified type parameter extension should call its Java counterpart`() {

        val id = 1L

        operations.findById<TestClass>(id)

        verify {
            operations.findById(id, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `findAllById(ids) with reified type parameter extension should call its Java counterpart`() {

        val ids = listOf(1L, 2L)

        operations.findAllById<TestClass>(ids)

        verify {
            operations.findAllById(ids, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `findAll() with reified type parameter extension should call its Java counterpart`() {

        operations.findAll<TestClass>()

        verify {
            operations.findAll(TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `findAll(Sort) with reified type parameter extension should call its Java counterpart`() {

        val sort = mockk<Sort>(relaxed = true)

        operations.findAll<TestClass>(sort)

        verify {
            operations.findAll(TestClass::class.java, sort)
        }
    }

    @Test // GH-1961
    fun `findAll(Pageable) with reified type parameter extension should call its Java counterpart`() {

        val pageable = mockk<Pageable>(relaxed = true)

        operations.findAll<TestClass>(pageable)

        verify {
            operations.findAll(TestClass::class.java, pageable)
        }
    }

    @Test // GH-1961
    fun `findOne(Query) with reified type parameter extension should call its Java counterpart`() {

        val query = mockk<Query>(relaxed = true)

        operations.findOne<TestClass>(query)

        verify {
            operations.findOne(query, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `findAll(Query) with reified type parameter extension should call its Java counterpart`() {

        val query = mockk<Query>(relaxed = true)

        operations.findAll<TestClass>(query)

        verify {
            operations.findAll(query, TestClass::class.java)
        }
    }


    @Test // GH-1961
    fun `findAll(Query, Pageable) with reified type parameter extension should call its Java counterpart`() {

        val query = mockk<Query>(relaxed = true)
        val pageable = mockk<Pageable>(relaxed = true)

        operations.findAll<TestClass>(query, pageable)

        verify {
            operations.findAll(query, TestClass::class.java, pageable)
        }
    }

    @Test // GH-1961
    fun `deleteById(id) with reified type parameter extension should call its Java counterpart`() {

        val id = 1L

        operations.deleteById<TestClass>(id)

        verify {
            operations.deleteById(id, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `deleteAllById(ids) with reified type parameter extension should call its Java counterpart`() {

        val ids = listOf(1L, 2L)

        operations.deleteAllById<TestClass>(ids)

        verify {
            operations.deleteAllById(ids, TestClass::class.java)
        }
    }

    @Test // GH-1961
    fun `deleteAll(ids) with reified type parameter extension should call its Java counterpart`() {

        operations.deleteAll<TestClass>()

        verify {
            operations.deleteAll(TestClass::class.java)
        }
    }
}