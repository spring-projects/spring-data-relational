/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import java.util.List;

import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface UserRepository extends CrudRepository<User, Integer> {

	// -------------------------------------------------------------------------
	// Derived Queries
	// -------------------------------------------------------------------------

	User findByFirstname(String name);

	// -------------------------------------------------------------------------
	// Declared Queries
	// -------------------------------------------------------------------------

	@Query("SELECT * FROM MY_USER WHERE firstname = :name")
	User findByFirstnameAnnotated(String name);

	@Query("SELECT * FROM MY_USER WHERE firstname = :#{#name}")
	User findByFirstnameExpression(String name);

	// -------------------------------------------------------------------------
	// Parameter naming
	// -------------------------------------------------------------------------

	@Query("select u from User u where u.lastname like :name or u.lastname like :name ORDER BY u.lastname")
	List<User> findAnnotatedWithParameterNameQuery(@Param("name") String lastname);

	List<User> findWithParameterNameByFirstnameStartingWithOrFirstnameEndingWith(@Param("l1") String l1,
			@Param("l2") String l2);

	// -------------------------------------------------------------------------
	// Named Queries
	// -------------------------------------------------------------------------

	User findByNamedQuery(String name);

	@Query(name = "User.findBySomeAnnotatedNamedQuery")
	User findByAnnotatedNamedQuery(String name);

}
