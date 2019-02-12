/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.domain;

import org.junit.Test;

import java.util.AbstractMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Jens Schauder
 */
public class IdentifierUnitTests {

	@Test // DATAJDBC-326
	public void getParametersByName() {

		Identifier identifier = Identifier.simple("aName", "aValue", String.class);;

		assertThat(identifier.getParametersByName())
				.containsExactly(new AbstractMap.SimpleEntry<>("aName", "aValue"));
	}
}