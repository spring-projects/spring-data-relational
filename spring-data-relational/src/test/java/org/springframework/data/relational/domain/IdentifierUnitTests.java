/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.domain;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

/**
 * Unit tests for {@link Identifier}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class IdentifierUnitTests {

	@Test // DATAJDBC-326
	public void getParametersByName() {

		Identifier identifier = Identifier.of("aName", "aValue", String.class);

		assertThat(identifier.toMap()).hasSize(1).containsEntry("aName", "aValue");
	}

	@Test // DATAJDBC-326
	public void parametersWithStringKeysUseObjectAsTypeForNull() {

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("one", null);

		Identifier identifier = Identifier.from(parameters);

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly( //
						tuple("one", null, Object.class) //
				);
	}

	@Test // DATAJDBC-326
	public void createsIdentifierFromMap() {

		Identifier identifier = Identifier.from(Collections.singletonMap("aName", "aValue"));

		assertThat(identifier.toMap()).hasSize(1).containsEntry("aName", "aValue");
	}

	@Test // DATAJDBC-326
	public void withAddsNewEntries() {

		Identifier identifier = Identifier.from(Collections.singletonMap("aName", "aValue")).withPart("foo", "bar",
				String.class);

		assertThat(identifier.toMap()).hasSize(2).containsEntry("aName", "aValue").containsEntry("foo", "bar");
	}

	@Test // DATAJDBC-326
	public void withOverridesExistingEntries() {

		Identifier identifier = Identifier.from(Collections.singletonMap("aName", "aValue")).withPart("aName", "bar",
				String.class);

		assertThat(identifier.toMap()).hasSize(1).containsEntry("aName", "bar");
	}

	@Test // DATAJDBC-326
	public void forEachIteratesOverKeys() {

		List<String> keys = new ArrayList<>();

		Identifier.from(Collections.singletonMap("aName", "aValue")).forEach((name, value, targetType) -> keys.add(name));

		assertThat(keys).containsOnly("aName");
	}

	@Test // DATAJDBC-326
	public void equalsConsidersEquality() {

		Identifier one = Identifier.from(Collections.singletonMap("aName", "aValue"));
		Identifier two = Identifier.from(Collections.singletonMap("aName", "aValue"));
		Identifier three = Identifier.from(Collections.singletonMap("aName", "different"));

		assertThat(one).isEqualTo(two);
		assertThat(one).isNotEqualTo(three);
	}
}
