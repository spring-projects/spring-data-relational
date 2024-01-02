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

import reactor.test.StepVerifier;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Disabled
//@formatter:off
// tag::class[]
@ExtendWith(SpringExtension.class)
@ContextConfiguration
class PersonRepositoryTests {

  @Autowired
  PersonRepository repository;

  @Test
  void readsAllEntitiesCorrectly() {

    repository.findAll()
      .as(StepVerifier::create)
      .expectNextCount(1)
      .verifyComplete();
  }

  @Test
  void readsEntitiesByNameCorrectly() {

    repository.findByFirstname("Hello World")
      .as(StepVerifier::create)
      .expectNextCount(1)
      .verifyComplete();
  }
}
// end::class[]
