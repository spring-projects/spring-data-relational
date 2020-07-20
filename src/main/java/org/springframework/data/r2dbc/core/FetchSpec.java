/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

/**
 * Contract for fetching results.
 *
 * @param <T> row result type.
 * @author Mark Paluch
 * @see RowsFetchSpec
 * @see UpdatedRowsFetchSpec
 * @deprecated since 1.2, use Spring's {@link org.springframework.r2dbc.core} support instead.
 */
@Deprecated
public interface FetchSpec<T> extends RowsFetchSpec<T>, UpdatedRowsFetchSpec {}
