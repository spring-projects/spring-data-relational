/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.function.Predicate;

/**
 * Collection of relational predicates.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class RelationalPredicates {

	/**
	 * Predicate to determine whether a property is a relation (i.e. it is an entity, not an identifier property, and not
	 * an embedded property).
	 *
	 * @return a predicate that tests if the given property is a relation.
	 */
	public static Predicate<? super RelationalPersistentProperty> isRelation() {
		return RelationalPredicates::isRelation;
	}

	/**
	 * Determine whether a property is a relation (i.e. it is an entity, not an identifier property, and not an embedded
	 * property).
	 *
	 * @return {@literal true} if the property is a relation; {@literal false} otherwise.
	 */
	public static boolean isRelation(RelationalPersistentProperty property) {
		return !property.isIdProperty() && property.isEntity() && !property.isEmbedded();
	}
}
