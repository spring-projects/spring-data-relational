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
package org.springframework.data.relational.core.conversion;

import static org.assertj.core.api.Java6Assertions.*;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for {@link EffectiveParentId}.
 *
 * @author Jens Schauder
 */
public class EffectiveParentIdTest {

	RelationalMappingContext context = new RelationalMappingContext();

	PersistentPropertyPath<RelationalPersistentProperty> root = null;
	PersistentPropertyPath<RelationalPersistentProperty> elements = context.getPersistentPropertyPath("elements",
			Root.class);
	PersistentPropertyPath<RelationalPersistentProperty> elementsChild = context
			.getPersistentPropertyPath("elements.child", Root.class);

	@Test // DATAJDBC-223
	public void firstLevel() {

		EffectiveParentId effectiveParentId = new EffectiveParentId();
		effectiveParentId.addKey(root, 23L);

		Map<String, Object> parameters = effectiveParentId.toParameterMap(elements);

		assertThat(parameters).containsExactly(new AbstractMap.SimpleEntry<>("root", 23L));
	}

	@Test // DATAJDBC-223
	public void secondLevel() {

		EffectiveParentId effectiveParentId = new EffectiveParentId();
		effectiveParentId.addKey(root, 23L);
		effectiveParentId.addKey(elements, 2L);

		Map<String, Object> parameters = effectiveParentId.toParameterMap(elements);

		assertThat(parameters).containsOnly( //
				new AbstractMap.SimpleEntry<>("root", 23L), //
				new AbstractMap.SimpleEntry<>("root_key", 2L) //
		);
	}

	@Test // DATAJDBC-223
	public void thirdLevel() {

		EffectiveParentId effectiveParentId = new EffectiveParentId();
		effectiveParentId.addKey(root, 23L);
		effectiveParentId.addKey(elements, 2L);

		Map<String, Object> parameters = effectiveParentId.toParameterMap(elementsChild);

		assertThat(parameters).containsOnly( //
				new AbstractMap.SimpleEntry<>("root", 23L), //
				new AbstractMap.SimpleEntry<>("root_key", 2L) //
		);
	}

	private static class Root {

		List<Element> elements;
	}

	private static class Element {

		Child child;
	}

	private static class Child {

		String content;
	}

}
