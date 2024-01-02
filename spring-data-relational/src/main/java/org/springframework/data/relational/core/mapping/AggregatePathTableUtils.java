/*
 * Copyright 2023-2024 the original author or authors.
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

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Utility methods to derive table and column information from {@link AggregatePath}.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class AggregatePathTableUtils {

	public static SqlIdentifier prefixWithTableAlias(AggregatePath path, SqlIdentifier columnName) {

		AggregatePath tableOwner = AggregatePathTraversal.getTableOwningPath(path);
		SqlIdentifier tableAlias = tableOwner.isRoot() ? null : constructTableAlias(tableOwner);

		return tableAlias == null ? columnName : columnName.transform(name -> tableAlias.getReference() + "_" + name);
	}

	public static SqlIdentifier constructTableAlias(AggregatePath path) {

		String alias = path.stream() //
				.filter(p -> !p.isRoot()) //
				.map(p -> p.isEmbedded() //
						? p.getRequiredLeafProperty().getEmbeddedPrefix()//
						: p.getRequiredLeafProperty().getName() + (p == path ? "" : "_") //
				) //
				.collect(new ReverseJoinCollector());
		return SqlIdentifier.quoted(alias);
	}

	private static class ReverseJoinCollector implements Collector<String, StringBuilder, String> {
		@Override
		public Supplier<StringBuilder> supplier() {
			return StringBuilder::new;
		}

		@Override
		public BiConsumer<StringBuilder, String> accumulator() {
			return ((stringBuilder, s) -> stringBuilder.insert(0, s));
		}

		@Override
		public BinaryOperator<StringBuilder> combiner() {
			return (a, b) -> b.append(a);
		}

		@Override
		public Function<StringBuilder, String> finisher() {
			return StringBuilder::toString;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return Collections.emptySet();
		}
	}
}
