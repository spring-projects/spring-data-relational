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
package org.springframework.data.jdbc.repository.aot;

import java.util.List;

import org.springframework.data.jdbc.repository.query.ParameterBinding;

/**
 * An AOT query represented by a string.
 *
 * @author Mark Paluch
 * @since 4.0
 */
abstract class StringAotQuery extends AotQuery {

	StringAotQuery(List<ParameterBinding> parameterBindings) {
		super(parameterBindings);
	}

	static StringAotQuery of(String query, List<ParameterBinding> parameterBindings) {
		return new DeclaredAotQuery(query, parameterBindings);
	}

	static StringAotQuery named(String queryName, String query, List<ParameterBinding> parameterBindings) {
		return new NamedStringAotQuery(queryName, query, parameterBindings);
	}

	public abstract String getQueryString();

	@Override
	public String toString() {
		return getQueryString();
	}

	/**
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 */
	private static class DeclaredAotQuery extends StringAotQuery {

		private final String query;

		DeclaredAotQuery(String query, List<ParameterBinding> parameterBindings) {
			super(parameterBindings);
			this.query = query;
		}

		@Override
		public String getQueryString() {
			return query;
		}

	}

	static class NamedStringAotQuery extends DeclaredAotQuery {

		private final String queryName;

		NamedStringAotQuery(String queryName, String query, List<ParameterBinding> parameterBindings) {
			super(query, parameterBindings);
			this.queryName = queryName;
		}

		public String getQueryName() {
			return queryName;
		}

	}

}
