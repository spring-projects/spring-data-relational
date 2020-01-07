/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.jdbc.core;

import org.springframework.data.jdbc.core.convert.DataAccessStrategy;

/**
 * Delegates all method calls to an instance set after construction. This is useful for {@link DataAccessStrategy}s with
 * cyclic dependencies.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @deprecated since 1.1, use {@link org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy}.
 */
@Deprecated
public class DelegatingDataAccessStrategy
		extends org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy {}
