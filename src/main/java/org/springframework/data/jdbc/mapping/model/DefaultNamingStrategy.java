/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.mapping.model;

/**
 * Basic implementation of {@link NamingStrategy} with no schema, table based on {@link Class} and
 * column name based on {@link JdbcPersistentProperty}.
 *
 * NOTE: Can also be used as an adapter. Create an anonymous subclass and override any settings to implement
 * a different strategy on the fly.
 *
 * @author Greg Turnquist
 * @author Michael Simons
 * @deprecated Use {@link NamingStrategy} for a default implementation and implement methods as needed
 */
public class DefaultNamingStrategy implements NamingStrategy {
}
