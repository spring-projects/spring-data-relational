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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.AbstractSqlParameterSource;

/**
 * Implementation of the {@link org.springframework.jdbc.core.namedparam.SqlParameterSource} interface based on
 * {@link SqlIdentifier} instead of {@link String} for names.
 *
 * @author Jens Schauder
 * @author Kurt Niemi
 * @author Mikhail Polivakha
 * @since 2.0
 */
class SqlIdentifierParameterSource extends AbstractSqlParameterSource {

	private final Set<SqlIdentifier> identifiers = new HashSet<>();
	private final Map<String, Object> namesToValues = new HashMap<>();

	@Override
	public boolean hasValue(String paramName) {
		return namesToValues.containsKey(paramName);
	}

	@Override
	public Object getValue(String paramName) throws IllegalArgumentException {
		return namesToValues.get(paramName);
	}

	@Override
	public String[] getParameterNames() {
		return namesToValues.keySet().toArray(new String[0]);
	}

	Set<SqlIdentifier> getIdentifiers() {
		return Collections.unmodifiableSet(identifiers);
	}

	void addValue(SqlIdentifier name, Object value) {
		addValue(name, value, Integer.MIN_VALUE);
	}

	void addValue(SqlIdentifier identifier, Object value, int sqlType) {

		identifiers.add(identifier);
		String name = BindParameterNameSanitizer.sanitize(identifier.getReference());
		namesToValues.put(name, value);
		registerSqlType(name, sqlType);
	}

	void addAll(SqlIdentifierParameterSource others) {

		for (SqlIdentifier identifier : others.getIdentifiers()) {

			String name = BindParameterNameSanitizer.sanitize( identifier.getReference());
			addValue(identifier, others.getValue(name), others.getSqlType(name));
		}
	}

	int size() {
		return namesToValues.size();
	}
}
