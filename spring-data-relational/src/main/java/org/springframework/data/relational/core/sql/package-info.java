/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Query Builder AST. Use {@link org.springframework.data.relational.core.sql.SQL} as entry point to create SQL objects. Objects and dependent objects created by the Query AST are immutable except for builders.
 * <p/> The Query Builder API is intended for framework usage to produce SQL required for framework operations.
 */
@NonNullApi
package org.springframework.data.relational.core.sql;

import org.springframework.lang.NonNullApi;
