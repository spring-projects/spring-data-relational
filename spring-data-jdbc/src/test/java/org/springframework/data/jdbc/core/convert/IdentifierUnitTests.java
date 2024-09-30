/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link Identifier}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class IdentifierUnitTests {

	@Test // DATAJDBC-326
	public void getParametersByName() {

		Identifier identifier = Identifier.of(unquoted("aName"), "aValue", String.class);

		assertThat(identifier.toMap()).hasSize(1).containsEntry(unquoted("aName"), "aValue");
	}

	@Test // DATAJDBC-326
	public void typeIsCalculatedCorrectly() {

		HashMap<SqlIdentifier, Object> parameters = new HashMap<>();
		Object objectValue = new Object();
		Object stringValue = "text";
		Object intValue	= 23;
		Object integerValue = 42;

		parameters.put(unquoted("one"), objectValue);
		parameters.put(unquoted("two"), stringValue);
		parameters.put(unquoted("three"), intValue);
		parameters.put(unquoted("four"), integerValue);

		Identifier identifier = Identifier.from(parameters);

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactlyInAnyOrder( //
						Assertions.tuple(unquoted("one"), objectValue, Object.class), //
						Assertions.tuple(unquoted("two"), stringValue, String.class), //
						Assertions.tuple(unquoted("three"), intValue, Integer.class), //
						Assertions.tuple(unquoted("four"), integerValue, Integer.class) //
				);
	}

	@Test // DATAJDBC-326
	public void createsIdentifierFromMap() {

		Identifier identifier = Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"));

		assertThat(identifier.toMap()).hasSize(1).containsEntry(unquoted("aName"), "aValue");
	}

	@Test // DATAJDBC-326
	public void withAddsNewEntries() {

		Identifier identifier = Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"))
				.withPart(unquoted("foo"), "bar", String.class);

		assertThat(identifier.toMap()) //
				.hasSize(2) //
				.containsEntry(unquoted("aName"), "aValue") //
				.containsEntry(unquoted("foo"), "bar");
	}

	@Test // DATAJDBC-326
	public void withOverridesExistingEntries() {

		Identifier identifier = Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"))
				.withPart(unquoted("aName"), "bar", String.class);

		assertThat(identifier.toMap()) //
				.hasSize(1) //
				.containsEntry(unquoted("aName"), "bar");
	}

	@Test // DATAJDBC-326
	public void forEachIteratesOverKeys() {

		List<String> keys = new ArrayList<>();

		Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"))
				.forEach((name, value, targetType) -> keys.add(name.toSql(IdentifierProcessing.ANSI)));

		assertThat(keys).containsOnly("aName");
	}

	@Test // DATAJDBC-326
	public void equalsConsidersEquality() {

		Identifier one = Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"));
		Identifier two = Identifier.from(Collections.singletonMap(unquoted("aName"), "aValue"));
		Identifier three = Identifier.from(Collections.singletonMap(unquoted("aName"), "different"));

		assertThat(one).isEqualTo(two);
		assertThat(one).isNotEqualTo(three);
	}

	@Test // DATAJDBC-542
	public void identifierPartsCanBeAccessedByString() {

		Map<SqlIdentifier, Object> idParts = new HashMap<>();
		idParts.put(unquoted("aName"), "one");
		idParts.put(quoted("Other"), "two");

		Identifier id = Identifier.from(idParts);

		Map<SqlIdentifier, Object> map = id.toMap();

		assertSoftly(softly -> {
			softly.assertThat(map.get("aName")).describedAs("aName").isEqualTo("one");
			softly.assertThat(map.get("Other")).describedAs("Other").isEqualTo("two");
			softly.assertThat(map.get("other")).describedAs("other").isNull();
			softly.assertThat(map.get("OTHER")).describedAs("OTHER").isNull();
		});
	}
}
