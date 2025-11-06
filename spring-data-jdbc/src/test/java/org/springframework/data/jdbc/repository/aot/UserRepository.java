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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.util.Streamable;

public interface UserRepository extends CrudRepository<User, Integer> {

	// -------------------------------------------------------------------------
	// Derived Queries
	// -------------------------------------------------------------------------

	User findByFirstname(String name);

	User findByFirstnameLike(String name);

	User findByFirstnameStartingWith(String name);

	User findByFirstnameEndingWith(String name);

	List<User> findByCreatedBefore(Instant instant);

	List<User> findByCreatedBetween(Instant from, Instant to);

	List<User> findByFriend(AggregateReference<User, Long> friend);

	List<User> findAllByAgeBetween(int start, int end);

	Streamable<User> findStreamableByAgeBetween(int start, int end);

	Optional<User> findOptionalByFirstname(String name);

	Stream<User> streamByAgeGreaterThan(int age);

	long countByAgeLessThan(int age);

	short countShortByAgeLessThan(int age);

	boolean existsByAgeLessThan(int age);

	List<User> findTop5ByOrderByAge();

	Slice<User> findSliceByAgeGreaterThan(Pageable pageable, int age);

	Page<User> findPageByAgeGreaterThan(Pageable pageable, int age);

	Streamable<User> findStreamableByAgeGreaterThan(Pageable pageable, int age);

	// -------------------------------------------------------------------------
	// Declared Queries
	// -------------------------------------------------------------------------

	@Query("SELECT * FROM MY_USER WHERE firstname = :name")
	User findByFirstnameAnnotated(String name);

	@Query("SELECT * FROM MY_USER WHERE firstname = :#{#name}")
	User findByFirstnameExpression(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE firstname = :name", rowMapperClass = MyRowMapper.class)
	User findUsingRowMapper(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE firstname = :name", rowMapperClass = MyRowMapper.class,
			resultSetExtractorClass = MyResultSetExtractor.class)
	User findUsingRowMapperAndResultSetExtractor(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE firstname = :name", rowMapperRef = "myRowMapper")
	User findUsingRowMapperRef(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE firstname = :name",
			resultSetExtractorClass = SimpleResultSetExtractor.class)
	int findUsingAndResultSetExtractor(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE firstname = :name", resultSetExtractorRef = "simpleResultSetExtractor")
	int findUsingAndResultSetExtractorRef(String name);

	@Query(value = "SELECT * FROM MY_USER WHERE created < :instant")
	List<User> findCreatedBefore(Instant instant);

	@Query(value = "SELECT * FROM MY_USER WHERE created < :instant")
	Streamable<User> findStreamableCreatedBefore(Instant instant);

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

	// -------------------------------------------------------------------------
	// Projections: DTO
	// -------------------------------------------------------------------------

	UserDto findOneDtoByFirstname(String name);

	List<UserDto> findDtoByFirstname(String name);

	// -------------------------------------------------------------------------
	// Projections: Interface
	// -------------------------------------------------------------------------

	UserProjection findOneInterfaceByFirstname(String name);

	List<UserProjection> findInterfaceByFirstname(String name);

	<T> List<T> findDynamicProjectionByFirstname(String name, Class<T> type);

	// -------------------------------------------------------------------------
	// Modifying
	// -------------------------------------------------------------------------

	boolean deleteByFirstname(String name);

	int deleteCountByFirstname(String name);

	User deleteOneByFirstname(String name);

	@Modifying
	@Query("delete from MY_USER where firstname = :firstname")
	int deleteAnnotatedQuery(String firstname);

	@Modifying
	@Query("delete from MY_USER where firstname = :firstname")
	void deleteWithoutResult(String firstname);

}
