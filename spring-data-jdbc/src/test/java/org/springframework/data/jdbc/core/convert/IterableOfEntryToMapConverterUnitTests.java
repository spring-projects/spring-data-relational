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
package org.springframework.data.jdbc.core.convert;

import static java.util.Arrays.*;
import static java.util.Collections.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.TypeDescriptor;

/**
 * Unit tests for {@link IterableOfEntryToMapConverter}.
 *
 * @author Jens Schauder
 */
public class IterableOfEntryToMapConverterUnitTests {

	IterableOfEntryToMapConverter converter = new IterableOfEntryToMapConverter();

	@Test
	public void testConversions() {

		Map<Object, Object> map = new HashMap<>();
		map.put("key", "value");
		List<Object[]> testValues = asList( //
				new Object[] { emptySet(), emptyMap() }, //
				new Object[] { emptyList(), emptyMap() }, //
				new Object[] { "string", false }, //
				new Object[] { asList(new SimpleEntry<>("key", "value")), map }, //
				new Object[] { asList("string"), IllegalArgumentException.class } //
		);

		SoftAssertions softly = new SoftAssertions();
		testValues.forEach(array -> softly.assertThat(tryToConvert(array[0])).isEqualTo(array[1]));
		softly.assertAll();
	}

	private Object tryToConvert(Object input) {

		try {
			if (converter.matches( //
					TypeDescriptor.valueOf(input.getClass()), //
					TypeDescriptor.valueOf(Map.class)) //
			) {
				return converter.convert((Iterable) input);
			} else {
				return false;
			}

		} catch (Exception e) {
			return e.getClass();
		}
	}
}
