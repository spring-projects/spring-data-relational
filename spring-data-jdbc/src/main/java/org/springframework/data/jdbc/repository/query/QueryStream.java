/*
 * Copyright 2018-2019 the original author or authors.
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

import java.lang.annotation.*;

/**
 * Annotation to mark a {@link org.springframework.data.jdbc.repository.query.Query} as
 * streamable. The resulting {@link java.util.stream.Stream} will wrap a <b>connected</b>
 * {@link java.sql.ResultSet} that will fetch rows as needed. It is responsibility of
 * the client code to close all resources via {@link java.util.stream.Stream#close()}.
 *
 * @author detinho
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface QueryStream {

    /**
     * The number of rows fetched from the database when more rows are needed
     */
    int fetchSize();

}
