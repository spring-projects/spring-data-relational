/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.springframework.data.domain.Sort.by;
import static org.springframework.data.domain.Sort.Order.*;
import static org.springframework.data.relational.core.query.Criteria.*;
import static org.springframework.data.relational.core.query.Query.*;
import static org.springframework.data.relational.core.query.Update.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;

/**
 * @author Mark Paluch
 */
//@formatter:off
class R2dbcEntityTemplateSnippets {

	void saveAndSelect(R2dbcEntityTemplate template) {

		// tag::insertAndSelect[]
		Person person = new Person("John", "Doe");

		Mono<Person> saved = template.insert(person);
		Mono<Person> loaded = template.selectOne(query(where("firstname").is("John")),
				Person.class);
		// end::insertAndSelect[]
	}


	void select(R2dbcEntityTemplate template) {

		// tag::select[]
		Flux<Person> loaded = template.select(query(where("firstname").is("John")),
				Person.class);
		// end::select[]
	}

	void simpleSelect(R2dbcEntityTemplate template) {

		// tag::simpleSelect[]
		Flux<Person> people = template.select(Person.class) // <1>
				.all(); // <2>
		// end::simpleSelect[]
	}

	void fullSelect(R2dbcEntityTemplate template) {

		// tag::fullSelect[]
		Mono<Person> first = template.select(Person.class)	// <1>
			.from("other_person")
			.matching(query(where("firstname").is("John")			// <2>
				.and("lastname").in("Doe", "White"))
			  .sort(by(desc("id"))))													// <3>
			.one();																						// <4>
		// end::fullSelect[]
	}

	void insert(R2dbcEntityTemplate template) {

		// tag::insert[]
		Mono<Person> insert = template.insert(Person.class)	// <1>
				.using(new Person("John", "Doe")); // <2>
		// end::insert[]
	}

	void fluentUpdate(R2dbcEntityTemplate template) {

		// tag::update[]
		Mono<Long> update = template.update(Person.class)	// <1>
				.inTable("other_table")														// <2>
				.matching(query(where("firstname").is("John")))		// <3>
				.apply(update("age", 42));												// <4>
		// end::update[]
	}

	void delete(R2dbcEntityTemplate template) {

		// tag::delete[]
		Mono<Long> delete = template.delete(Person.class)	// <1>
				.from("other_table")															// <2>
				.matching(query(where("firstname").is("John")))		// <3>
				.all();																						// <4>
		// end::delete[]
	}

	static class Person {
		String firstname, lastname;
		public Person(String firstname, String lastname) {
	this.firstname = firstname;
	this.lastname = lastname;
}}
}
