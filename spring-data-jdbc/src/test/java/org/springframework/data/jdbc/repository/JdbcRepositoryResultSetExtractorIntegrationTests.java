/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Very simple use cases for creation and usage of {@link ResultSetExtractor}s in JdbcRepository.
 *
 * @author Evgeni Dimitrov
 */
@IntegrationTest
public class JdbcRepositoryResultSetExtractorIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		PersonRepository personEntityRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(PersonRepository.class);
		}

	}

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired PersonRepository personRepository;

	@Test // DATAJDBC-290
	public void findAllPeopleWithAdressesReturnsEmptyWhenNoneFound() {

		// NOT saving anything, so DB is empty

		assertThat(personRepository.findAllPeopleWithAddresses()).isEmpty();
	}

	@Test // DATAJDBC-290
	public void findAllPeopleWithAddressesReturnsOnePersonWithoutAddresses() {

		personRepository.save(new Person(null, "Joe", null));

		assertThat(personRepository.findAllPeopleWithAddresses()).hasSize(1);
	}

	@Test // DATAJDBC-290
	public void findAllPeopleWithAddressesReturnsOnePersonWithAddresses() {

		final String personName = "Joe";
		Person savedPerson = personRepository.save(new Person(null, personName, null));

		String street1 = "Some Street";
		String street2 = "Some other Street";

		MapSqlParameterSource paramsAddress1 = buildAddressParameters(savedPerson.getId(), street1);
		template.update("insert into address (street, person_id) values (:street, :personId)", paramsAddress1);

		MapSqlParameterSource paramsAddress2 = buildAddressParameters(savedPerson.getId(), street2);
		template.update("insert into address (street, person_id) values (:street, :personId)", paramsAddress2);

		List<Person> people = personRepository.findAllPeopleWithAddresses();

		assertThat(people).hasSize(1);
		Person person = people.get(0);
		assertThat(person.getName()).isEqualTo(personName);
		assertThat(person.getAddresses()).hasSize(2);
		assertThat(person.getAddresses()).extracting(a -> a.getStreet()).containsExactlyInAnyOrder(street1, street2);
	}

	private MapSqlParameterSource buildAddressParameters(Long id, String streetName) {

		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("street", streetName, Types.VARCHAR);
		params.addValue("personId", id, Types.NUMERIC);

		return params;
	}

	interface PersonRepository extends CrudRepository<Person, Long> {

		@Query(
				value = "select p.id, p.name, a.id addrId, a.street from person p left join address a on(p.id = a.person_id)",
				resultSetExtractorClass = PersonResultSetExtractor.class)
		List<Person> findAllPeopleWithAddresses();
	}

	static class Person {

		@Id
		private Long id;
		private String name;
		private List<Address> addresses;

		public Person(Long id, String name, List<Address> addresses) {
			this.id = id;
			this.name = name;
			this.addresses = addresses;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public List<Address> getAddresses() {
			return this.addresses;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setAddresses(List<Address> addresses) {
			this.addresses = addresses;
		}
	}

	static class Address {

		@Id
		private Long id;
		private String street;

		public Address(Long id, String street) {
			this.id = id;
			this.street = street;
		}

		public Long getId() {
			return this.id;
		}

		public String getStreet() {
			return this.street;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setStreet(String street) {
			this.street = street;
		}
	}

	static class PersonResultSetExtractor implements ResultSetExtractor<List<Person>> {

		@Override
		public List<Person> extractData(ResultSet rs) throws SQLException, DataAccessException {

			Map<Long, Person> peopleById = new HashMap<>();

			while (rs.next()) {

				long personId = rs.getLong("id");
				Person currentPerson = peopleById.computeIfAbsent(personId, t -> {

					try {
						return new Person(personId, rs.getString("name"), new ArrayList<>());
					} catch (SQLException e) {
						throw new RecoverableDataAccessException("Error mapping Person", e);
					}
				});

				if (currentPerson.getAddresses() == null) {
					currentPerson.setAddresses(new ArrayList<>());
				}

				long addrId = rs.getLong("addrId");
				if (!rs.wasNull()) {
					currentPerson.getAddresses().add(new Address(addrId, rs.getString("street")));
				}
			}

			return new ArrayList<>(peopleById.values());
		}

	}
}
