/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Value object encapsulating a query containing named parameters and a{@link SqlParameterSource} to bind the
 * parameters.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
public class ParametrizedQuery {

	private final String query;
	private final SqlParameterSource parameterSource;
	private final Criteria criteria;

	ParametrizedQuery(String query, SqlParameterSource parameterSource, Criteria criteria) {

		this.query = query;
		this.parameterSource = parameterSource;
		this.criteria = criteria;
	}

	public SqlParameterSource getParameterSource() {
		return parameterSource;
	}

	public String getQuery() {
		return query;
	}

	public Criteria getCriteria() {
		return criteria;
	}

	SqlParameterSource getParameterSource(Escaper escaper) {
		return new EscapingParameterSource(parameterSource, escaper);
	}

	@Override
	public String toString() {
		return this.query;
	}
}
