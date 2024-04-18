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

import io.mockk.mockk
import io.mockk.verify
import io.r2dbc.spi.Row
import org.junit.Test

/**
 * Unit tests for [RowExtensions].
 *
 * @author George Papadopoulos
 */
class RowExtensionsUnitTests {

    private val columnName = "random_column"
    private val columnIndex = 12

    private val row = mockk<Row>(relaxed = true)

    @Test
    fun `getColumnByName should call its Java counterpart with Integer`() {
        row.getColumn<Int>(columnName)
        verify { row.get(columnName, Integer::class.java) }
    }

    @Test
    fun `getColumnByName should call its Java counterpart`() {
        row.getColumn<String>(columnName)
        verify { row.get(columnName, String::class.java) }
    }

    @Test
    fun `getColumnNullableByName should call its Java counterpart`() {
        row.getColumnNullable<String>(columnName)
        verify { row.get(columnName, String::class.java) }
    }

    @Test
    fun `getColumnByIndex should call its Java counterpart`() {
        row.getColumn<String>(columnIndex)
        verify { row.get(columnIndex, String::class.java) }
    }


    @Test
    fun `getColumnNullableByIndex should call its Java counterpart`() {
        row.getColumnNullable<String>(columnIndex)
        verify { row.get(columnIndex, String::class.java) }
    }
}