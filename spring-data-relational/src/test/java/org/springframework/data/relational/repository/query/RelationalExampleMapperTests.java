/*
 * Copyright 2021-2024 the original author or authors.
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

package org.springframework.data.relational.repository.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.query.Query;

import java.util.Objects;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.ExampleMatcher.GenericPropertyMatchers.*;
import static org.springframework.data.domain.ExampleMatcher.StringMatcher.*;
import static org.springframework.data.domain.ExampleMatcher.*;

/**
 * Verify that the {@link RelationalExampleMapper} properly turns {@link Example}s into {@link Query}'s.
 *
 * @author Greg Turnquist
 */
public class RelationalExampleMapperTests {

	RelationalExampleMapper exampleMapper;

	@BeforeEach
	public void before() {
		exampleMapper = new RelationalExampleMapper(new RelationalMappingContext());
	}

	@Test // GH-929
	void queryByExampleWithId() {

		Person person = new Person();
		person.setId("id1");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Objects::toString) //
				.hasValue("(id = 'id1')");
	}

	@Test // GH-929
	void queryByExampleWithFirstname() {

		Person person = new Person();
		person.setFirstname("Frodo");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);
		assertThat(query.getCriteria().map(Object::toString).get()) //
				.contains("(firstname = 'Frodo')", //
						" AND ", //
						"(lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleWithNullMatchingLastName() {

		Person person = new Person();
		person.setLastname("Baggins");

		ExampleMatcher matcher = matching().withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleWithNullMatchingFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Bilbo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matching().withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);
		assertThat(query.getCriteria().map(Object::toString).get()) //
				.contains("(firstname IS NULL OR firstname = 'Bilbo')", //
						" AND ", //
						"(lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameAndLastnameIgnoringFirstname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matching().withIgnorePaths("firstname");
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameAndLastnameWithNullMatchingIgnoringFirstName() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matching().withIncludeNullValues().withIgnorePaths("firstname");
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingAtTheBeginning() {

		Person person = new Person();
		person.setFirstname("Fro");

		ExampleMatcher matcher = matching().withStringMatcher(STARTING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE 'Fro%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingOnTheEnding() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(ENDING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingContaining() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(CONTAINING);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingRegEx() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(ExampleMatcher.StringMatcher.REGEX);
		Example<Person> example = Example.of(person, matcher);

		assertThatIllegalStateException().isThrownBy(() -> exampleMapper.getMappedExample(example))
				.withMessageContaining("REGEX is not supported");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherEndsWith() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withMatcher("firstname", endsWith());
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherStartsWith() {

		Person person = new Person();
		person.setFirstname("Fro");

		ExampleMatcher matcher = matching().withMatcher("firstname", startsWith());
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE 'Fro%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithFieldSpecificStringMatcherContains() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withMatcher("firstname", contains());
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE '%do%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingAtTheBeginningIncludingNull() {

		Person person = new Person();
		person.setFirstname("Fro");

		ExampleMatcher matcher = matching().withStringMatcher(STARTING).withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname IS NULL OR firstname LIKE 'Fro%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingOnTheEndingIncludingNull() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(ENDING).withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname IS NULL OR firstname LIKE '%do')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameIgnoreCaseFieldLevel() {

		Person person = new Person();
		person.setFirstname("fro");

		ExampleMatcher matcher = matching().withMatcher("firstname", startsWith().ignoreCase());
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname LIKE 'fro%')");

		assertThat(example.getMatcher().getPropertySpecifiers().getForPath("firstname").getIgnoreCase()).isTrue();
	}

	@Test // GH-929
	void queryByExampleWithFirstnameWithStringMatchingContainingIncludingNull() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(CONTAINING).withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname IS NULL OR firstname LIKE '%do%')");
	}

	@Test // GH-929
	void queryByExampleWithFirstnameIgnoreCase() {

		Person person = new Person();
		person.setFirstname("Frodo");

		ExampleMatcher matcher = matching().withIgnoreCase(true);
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");

		assertThat(example.getMatcher().isIgnoreCaseEnabled()).isTrue();
	}

	@Test // GH-929
	void queryByExampleWithFirstnameOrLastname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matchingAny();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);
		assertThat(query.getCriteria().map(Object::toString).get()) //
				.contains("(firstname = 'Frodo')", //
						" OR ", //
						"(lastname = 'Baggins')");
	}

	@Test // GH-929
	void queryByExampleEvenHandlesInvisibleFields() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setSecret("I have the ring!");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria().map(Object::toString).get()) //
				.contains("(firstname = 'Frodo')", //
						" AND ", //
						"(secret = 'I have the ring!')");
	}

	@Test // GH-929
	void queryByExampleSupportsPropertyTransforms() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");
		person.setSecret("I have the ring!");

		ExampleMatcher matcher = matching() //
				.withTransformer("firstname", o -> {
					if (o.isPresent()) {
						return o.map(o1 -> ((String) o1).toUpperCase());
					}
					return o;
				}) //
				.withTransformer("lastname", o -> {
					if (o.isPresent()) {
						return o.map(o1 -> ((String) o1).toLowerCase());
					}
					return o;
				});

		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria().map(Object::toString).get()) //
				.contains("(firstname = 'FRODO')", //
						" AND ", //
						"(lastname = 'baggins')", //
						"(secret = 'I have the ring!')");
	}

	static class Person {

		@Id
		String id;
		String firstname;
		String lastname;
		String secret;

		public Person(String id, String firstname, String lastname, String secret) {
			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.secret = secret;
		}

		public Person() {
		}

		// Override default visibility of getting the secret.
		private String getSecret() {
			return this.secret;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public void setSecret(String secret) {
			this.secret = secret;
		}
	}
}
