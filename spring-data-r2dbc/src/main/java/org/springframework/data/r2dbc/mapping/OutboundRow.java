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
package org.springframework.data.r2dbc.mapping;

import io.r2dbc.spi.Row;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.Assert;

/**
 * Representation of a {@link Row} to be written through a {@code INSERT} or {@code UPDATE} statement. Row keys are
 * represented as {@link SqlIdentifier}. {@link String} key names are translated to
 * {@link SqlIdentifier#unquoted(String) unquoted identifiers} when adding or querying for entries.
 *
 * @author Mark Paluch
 * @see SqlIdentifier
 * @see Parameter
 */
public class OutboundRow implements Map<SqlIdentifier, Parameter>, Cloneable {

	private final Map<SqlIdentifier, Parameter> rowAsMap;

	/**
	 * Creates an empty {@link OutboundRow} instance.
	 */
	public OutboundRow() {
		this.rowAsMap = new LinkedHashMap<>();
	}

	/**
	 * Creates a new {@link OutboundRow} from a {@link Map}.
	 *
	 * @param map the map used to initialize the {@link OutboundRow}.
	 */
	public OutboundRow(Map<String, Parameter> map) {

		Assert.notNull(map, "Map must not be null");

		this.rowAsMap = new LinkedHashMap<>(map.size());

		map.forEach((s, Parameter) -> this.rowAsMap.put(SqlIdentifier.unquoted(s), Parameter));
	}

	private OutboundRow(OutboundRow map) {

		this.rowAsMap = new LinkedHashMap<>(map.size());
		this.rowAsMap.putAll(map);
	}

	/**
	 * Create a {@link OutboundRow} instance initialized with the given key/value pair.
	 *
	 * @param key key.
	 * @param value value.
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRow(String key, Parameter value) {
		this(SqlIdentifier.unquoted(key), value);
	}

	/**
	 * Create a {@link OutboundRow} instance initialized with the given key/value pair.
	 *
	 * @param key key.
	 * @param value value.
	 * @since 1.1
	 */
	public OutboundRow(SqlIdentifier key, Parameter value) {
		this.rowAsMap = new LinkedHashMap<>();
		this.rowAsMap.put(key, value);
	}

	/**
	 * Put the given key/value pair into this {@link OutboundRow} and return this. Useful for chaining puts in a single
	 * expression:
	 *
	 * <pre class="code">
	 * row.append("a", 1).append("b", 2)}
	 * </pre>
	 *
	 * @param key key.
	 * @param value value.
	 * @return this
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRow append(String key, Parameter value) {
		return append(SqlIdentifier.unquoted(key), value);
	}

	/**
	 * Put the given key/value pair into this {@link OutboundRow} and return this. Useful for chaining puts in a single
	 * expression:
	 *
	 * <pre class="code">
	 * row.append("a", 1).append("b", 2)}
	 * </pre>
	 *
	 * @param key key.
	 * @param value value.
	 * @return this
	 * @since 1.1
	 */
	public OutboundRow append(SqlIdentifier key, Parameter value) {
		this.rowAsMap.put(key, value);
		return this;
	}

	@Override
	public int size() {
		return this.rowAsMap.size();
	}

	@Override
	public boolean isEmpty() {
		return this.rowAsMap.isEmpty();
	}

	@Override
	protected OutboundRow clone() {
		return new OutboundRow(this);
	}

	@Override
	public boolean containsKey(Object key) {
		return this.rowAsMap.containsKey(convertKeyIfNecessary(key));
	}

	@Override
	public boolean containsValue(Object value) {
		return this.rowAsMap.containsValue(value);
	}

	@Override
	public Parameter get(Object key) {
		return this.rowAsMap.get(convertKeyIfNecessary(key));
	}

	public Parameter put(String key, Parameter value) {
		return put(SqlIdentifier.unquoted(key), value);
	}

	@Override
	public Parameter put(SqlIdentifier key, Parameter value) {
		return this.rowAsMap.put(key, value);
	}

	@Override
	public Parameter remove(Object key) {
		return this.rowAsMap.remove(key);
	}

	@Override
	public void putAll(Map<? extends SqlIdentifier, ? extends Parameter> m) {
		this.rowAsMap.putAll(m);
	}

	@Override
	public void clear() {
		this.rowAsMap.clear();
	}

	@Override
	public Set<SqlIdentifier> keySet() {
		return this.rowAsMap.keySet();
	}

	@Override
	public Collection<Parameter> values() {
		return this.rowAsMap.values();
	}

	@Override
	public Set<Entry<SqlIdentifier, Parameter>> entrySet() {
		return this.rowAsMap.entrySet();
	}

	@Override
	public boolean equals(@Nullable final Object o) {

		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		OutboundRow row = (OutboundRow) o;

		return this.rowAsMap.equals(row.rowAsMap);
	}

	@Override
	public int hashCode() {
		return this.rowAsMap.hashCode();
	}

	@Override
	public String toString() {
		return "OutboundRow[" + this.rowAsMap + "]";
	}

	@Override
	public void forEach(BiConsumer<? super SqlIdentifier, ? super Parameter> action) {
		this.rowAsMap.forEach(action);
	}

	private static Object convertKeyIfNecessary(Object key) {
		return key instanceof String keyString ? SqlIdentifier.unquoted(keyString) : key;
	}
}
