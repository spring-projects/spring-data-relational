/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates a query method should be considered a modifying query that returns nothing or the number of rows affected
 * by the query.
 * <p>
 * Query methods annotated with {@code @Modifying} are typically {@code INSERT}, {@code UPDATE}, {@code DELETE}, and DDL
 * statements that do not return tabular results. This annotation isn't applicable if the query method returns results
 * such as {@code INSERT} with generated keys.
 *
 * @author Mark Paluch
 * @see io.r2dbc.spi.Result#getRowsUpdated()
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface Modifying {
}
