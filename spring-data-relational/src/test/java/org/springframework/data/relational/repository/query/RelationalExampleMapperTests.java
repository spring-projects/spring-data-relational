/*
 * Copyright 2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.domain.ExampleMatcher.GenericPropertyMatchers.*;
import static org.springframework.data.domain.ExampleMatcher.StringMatcher.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.query.Query;

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

	@Test // #929
	void queryByExampleWithId() {

		Person person = new Person();
		person.setId("id1");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Objects::toString) //
				.hasValue("(id = 'id1')");
	}

	@Test // #929
	void queryByExampleWithFirstname() {

		Person person = new Person();
		person.setFirstname("Frodo");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");
	}

	@Test // #929
	void queryByExampleWithFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo') AND (lastname = 'Baggins')");
	}

	@Test // #929
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

	@Test // #929
	void queryByExampleWithNullMatchingFirstnameAndLastname() {

		Person person = new Person();
		person.setFirstname("Bilbo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matching().withIncludeNullValues();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname IS NULL OR firstname = 'Bilbo') AND (lastname IS NULL OR lastname = 'Baggins')");
	}

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
	void queryByExampleWithFirstnameWithStringMatchingRegEx() {

		Person person = new Person();
		person.setFirstname("do");

		ExampleMatcher matcher = matching().withStringMatcher(ExampleMatcher.StringMatcher.REGEX);
		Example<Person> example = Example.of(person, matcher);

		assertThatIllegalStateException().isThrownBy(() -> exampleMapper.getMappedExample(example))
				.withMessageContaining("REGEX is not supported!");
	}

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
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

	@Test // #929
	void queryByExampleWithFirstnameOrLastname() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setLastname("Baggins");

		ExampleMatcher matcher = matchingAny();
		Example<Person> example = Example.of(person, matcher);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo') OR (lastname = 'Baggins')");
	}

	@Test // #929
	void queryByExampleIgnoresInvisibleFields() {

		Person person = new Person();
		person.setFirstname("Frodo");
		person.setSecret("I have the ring!");

		Example<Person> example = Example.of(person);

		Query query = exampleMapper.getMappedExample(example);

		assertThat(query.getCriteria()) //
				.map(Object::toString) //
				.hasValue("(firstname = 'Frodo')");
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Person {

		@Id String id;
		String firstname;
		String lastname;
		String secret;

		// Override default visibility of getting the secret.
		private String getSecret() {
			return this.secret;
		}
	}
}
