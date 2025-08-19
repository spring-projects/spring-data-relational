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

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.data.repository.aot.generate.QueryMetadata;

/**
 * Value object capturing queries used for repository query methods.
 *
 * @author Mark Paluch
 * @since 4.0
 */
record AotQueries(AotQuery result, @Nullable AotQuery count) {

	/**
	 * Factory method to create an {@link AotQueries} instance with a single query.
	 *
	 * @param query
	 * @return
	 */
	public static AotQueries create(AotQuery query) {
		return new AotQueries(query, null);
	}

	/**
	 * Factory method to create an {@link AotQueries} instance with an entity- and a count query.
	 *
	 * @param query
	 * @param count
	 * @return
	 */
	public static AotQueries create(AotQuery query, AotQuery count) {
		return new AotQueries(query, count);
	}

	public QueryMetadata toMetadata() {
		return new AotQueryMetadata();
	}

	/**
	 * String and Named Query-based {@link QueryMetadata}.
	 */
	private class AotQueryMetadata implements QueryMetadata {

		@Override
		public Map<String, Object> serialize() {

			Map<String, Object> serialized = new LinkedHashMap<>();

			if (result() instanceof StringAotQuery sq) {
				serialized.put("query", sq.getQueryString());
			}

			if (result() instanceof StringAotQuery.NamedStringAotQuery nsq) {
				serialized.put("name", nsq.getQueryName());
			}

			if (count() instanceof StringAotQuery sq) {
				serialized.put("count-query", sq.getQueryString());
			}

			if (count() instanceof StringAotQuery.NamedStringAotQuery nsq) {
				serialized.put("count-name", nsq.getQueryName());
			}

			return serialized;
		}

	}

}
