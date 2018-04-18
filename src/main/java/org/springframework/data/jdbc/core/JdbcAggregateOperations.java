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
package org.springframework.data.jdbc.core;

/**
 * Specifies a operations one can perform on a database, based on an <em>Domain Type</em>.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public interface JdbcAggregateOperations {

	<T> void save(T instance);

	<T> void deleteById(Object id, Class<T> domainType);

	<T> void delete(T entity, Class<T> domainType);

	void deleteAll(Class<?> domainType);

	long count(Class<?> domainType);

	<T> T findById(Object id, Class<T> domainType);

	<T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType);

	<T> Iterable<T> findAll(Class<T> domainType);

	<T> boolean existsById(Object id, Class<T> domainType);
}
