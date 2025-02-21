/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * Factory to create a {@link RowMapper} for a given class.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 *
 * @since 2.3
 */
public interface RowMapperFactory {

    /**
     * Obtain a {@link RowMapper} based on the expected return type passed in as an argument.
     *
     * @param result must not be {@code null}.
     * @return a {@code RowMapper} producing instances of {@code result}.
     */
    RowMapper<Object> create(Class<?> result);

    /**
     * Obtain a {@link RowMapper}  from some other source, typically a {@link org.springframework.beans.factory.BeanFactory}.
     *
     * @param reference must not be {@code null}.
     * @since 3.4
     */
    default RowMapper<Object> getRowMapper(String reference) {
        throw new UnsupportedOperationException("getRowMapper by reference is not supported");
    }

    /**
     * Obtain a {@code ResultSetExtractor} from some other source, typically a {@link org.springframework.beans.factory.BeanFactory}.
     *
     * @param reference must not be {@code null}.
     * @since 3.4
     */
    default ResultSetExtractor<Object> getResultSetExtractor(String reference) {
        throw new UnsupportedOperationException("getResultSetExtractor by reference is not supported");
    }
}
