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
package org.springframework.data.r2dbc.core;

/**
 * Stripped down interface providing access to a fluent API that specifies a basic set of reactive R2DBC operations.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see R2dbcEntityOperations
 */
public interface FluentR2dbcOperations
		extends ReactiveSelectOperation, ReactiveInsertOperation, ReactiveUpdateOperation, ReactiveDeleteOperation {}
