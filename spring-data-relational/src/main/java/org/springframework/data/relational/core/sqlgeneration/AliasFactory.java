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
package org.springframework.data.relational.core.sqlgeneration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.AggregatePathTraversal;

/**
 * Creates aliases to be used in SQL generation
 *
 * @author Jens Schauder
 * @since 3.2
 */
public class AliasFactory {
	private final SingleAliasFactory columnAliases = new SingleAliasFactory("c");
	private final SingleAliasFactory tableAliases = new SingleAliasFactory("t");
	private final SingleAliasFactory rowNumberAliases = new SingleAliasFactory("rn");
	private final SingleAliasFactory rowCountAliases = new SingleAliasFactory("rc");
	private final SingleAliasFactory backReferenceAliases = new SingleAliasFactory("br");
	private final SingleAliasFactory keyAliases = new SingleAliasFactory("key");
	private final AtomicInteger counter = new AtomicInteger();

	private static String sanitize(String name) {
		return name.replaceAll("\\W", "");
	}

	public String getColumnAlias(AggregatePath path) {
		return columnAliases.getOrCreateFor(path);
	}

	public String getTableAlias(AggregatePath path) {
		return tableAliases.getOrCreateFor(path);
	}

	public String getRowNumberAlias(AggregatePath path) {
		return rowNumberAliases.getOrCreateFor(AggregatePathTraversal.getTableOwningPath(path));
	}

	public String getRowCountAlias(AggregatePath path) {
		return rowCountAliases.getOrCreateFor(path);
	}

	public String getBackReferenceAlias(AggregatePath path) {
		return backReferenceAliases.getOrCreateFor(path);
	}

	public String getKeyAlias(AggregatePath path) {
		return keyAliases.getOrCreateFor(path);
	}

	private class SingleAliasFactory {
		private final String prefix;
		private final Map<AggregatePath, String> cache = new ConcurrentHashMap<>();

		SingleAliasFactory(String prefix) {
			this.prefix = prefix + "_";
		}

		String getOrCreateFor(AggregatePath path) {
			return cache.computeIfAbsent(path, this::createName);
		}

		private String createName(AggregatePath path) {
			return prefix + getName(path) + "_" + (counter.incrementAndGet());
		}
	}

	private static String getName(AggregatePath path) {
		return sanitize( //
				path.isEntity() //
						? path.getTableInfo().qualifiedTableName().getReference() //
						: path.getColumnInfo().name().getReference()) //
				.toLowerCase();
	}

}
