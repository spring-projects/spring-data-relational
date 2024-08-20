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
package org.springframework.data.r2dbc.documentation;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface PersonRepository extends ReactiveCrudRepository<Person, String> {

	Flux<Person> findByFirstname(String firstname);

	// tag::atModifying[]
	@Modifying
	@Query("UPDATE person SET firstname = :firstname where lastname = :lastname")
	Mono<Integer> setFixedFirstnameFor(String firstname, String lastname);
	// end::atModifying[]

	// tag::spel[]
	@Query("SELECT * FROM person WHERE lastname = :#{[0]}")
	Flux<Person> findByQueryWithParameterExpression(String lastname);
	// end::spel[]

	// tag::spel2[]
	@Query("SELECT * FROM #{tableName} WHERE lastname = :lastname")
	Flux<Person> findByQueryWithExpression(String lastname);
	// end::spel2[]
}
