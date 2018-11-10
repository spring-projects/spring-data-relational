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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Very simple use cases for creation and usage of {@link ResultSetExtractor}s in JdbcRepository.
 *
 * @author Evgeni Dimitrov
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryIntegrationTestsResultSetExtractor {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryIntegrationTestsResultSetExtractor.class;
		}

		@Bean
		PersonRepository dummyEntityRepository() {
			return factory.getRepository(PersonRepository.class);
		}

	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired NamedParameterJdbcTemplate template;
	@Autowired PersonRepository repository;

	@Test // DATAJDBC-290
	public void findAllPeopleWithAdressesReturnsEmptyWhenNoneFound() {
		// NOT saving anything, so DB is empty
		assertThat(repository.findAllPeopleWithAdresses()).isEmpty();
	}
	
	@Test // DATAJDBC-290
	public void findAllPeopleWithAdressesReturnsOnePersonWithoutAdresses() {
		repository.save(new Person(null, "Joe", null));
		assertThat(repository.findAllPeopleWithAdresses()).hasSize(1);
	}
	
	@Test // DATAJDBC-290 
	public void findAllPeopleWithAdressesReturnsOnePersonWithAdresses() {
		Person savedPerson = repository.save(new Person(null, "Joe", null));
		MapSqlParameterSource paramsAddress1 = buildAddressParameters(savedPerson.getId(), "Klokotnitsa");
		template.update("insert into address (street, person_id) values (:street, :personId)",paramsAddress1);
		MapSqlParameterSource paramsAddress2 = buildAddressParameters(savedPerson.getId(), "bul. Hristo Botev");
		template.update("insert into address (street, person_id) values (:street, :personId)",paramsAddress2);
		
		List<Person> people = repository.findAllPeopleWithAdresses();
		assertThat(people).hasSize(1);
		assertThat(people.get(0).getAdresses()).hasSize(2);
	}

	private MapSqlParameterSource buildAddressParameters(Long id, String streetName) {
		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("street", streetName, Types.VARCHAR);
		params.addValue("personId", id, Types.NUMERIC);
		return params;
	}

	interface PersonRepository extends CrudRepository<Person, Long> {
		@Query(value="select p.id, p.name, a.id addrId, a.street from person p left join address a on(p.id = a.person_id)", 
				resultSetExtractorClass=PersonResultSetExtractor.class)
		public List<Person> findAllPeopleWithAdresses();
	}

	@Data
	@AllArgsConstructor
	static class Person {
		@Id 
		private Long id;
		private String name;
		private List<Address> adresses;
	}
	
	@Data
	@AllArgsConstructor
	static class Address {
		@Id 
		private Long id;
		private String street;
	}
	
	static class PersonResultSetExtractor implements ResultSetExtractor<List<Person>> {

		@Override
		public List<Person> extractData(ResultSet rs) throws SQLException, DataAccessException {
			Map<Long, Person> peopleById = new HashMap<>();
			while(rs.next()) {
				long personId = rs.getLong("id");
				Person currentPerson = peopleById.computeIfAbsent(personId, t -> {
					try {
						return new Person(personId, rs.getString("name"), new ArrayList<>());
					} catch (SQLException e) {
						throw new RecoverableDataAccessException("Error mapping Person", e);
					}
				}); 
				
				if(currentPerson.getAdresses() == null) currentPerson.setAdresses(new ArrayList<>());
				long addrId = rs.getLong("addrId");
				if(!rs.wasNull()) {
					currentPerson.getAdresses().add(new Address(addrId, rs.getString("street")));
				}
			}
			
			return new ArrayList<Person>(peopleById.values());
		}
		
	}
}
