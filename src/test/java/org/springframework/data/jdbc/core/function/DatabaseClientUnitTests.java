/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.junit.Test;

/**
 * @author Mark Paluch
 */
public class DatabaseClientUnitTests {

	@Test
	public void generic() {

		DatabaseClient c = null;

		Mono<Person> first = c.select().from(Person.class).fetch().first();

		Mono<String> project = c.select().from(Person.class).as(String.class).fetch().first();

		Flux<String> foo = c.select().from(Person.class).exchange()
				.flatMapMany(it -> it.extract((r, md) -> r.get("foo", String.class)).all());
	}

	@Test
	public void execute() {

		DatabaseClient c = null;

		Mono<Person> first = c.execute().sql("SELECT 1 FROM dual;").as(Person.class).fetch().first();
	}

	static class Person {

	}
}
