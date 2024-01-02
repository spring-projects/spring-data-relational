/*
 * Copyright 2018-2024 the original author or authors.
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

import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.ReactiveQueryByExampleExecutor;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;

/**
 * R2DBC specific {@link org.springframework.data.repository.Repository} interface with reactive support.
 *
 * @author Mark Paluch
 * @author Stephen Cohen
 * @author Greg Turnquist
 */
@NoRepositoryBean
public interface R2dbcRepository<T, ID> extends ReactiveCrudRepository<T, ID>, ReactiveSortingRepository<T, ID>, ReactiveQueryByExampleExecutor<T> {}
