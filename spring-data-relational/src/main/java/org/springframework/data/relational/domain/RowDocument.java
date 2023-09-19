/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.relational.domain;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.LinkedCaseInsensitiveMap;
import org.springframework.util.ObjectUtils;

/**
 * Represents a tabular structure as document to enable hierarchical traversal of SQL results.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public class RowDocument implements Map<String, Object> {

	private final Map<String, Object> delegate;

	public RowDocument() {
		this.delegate = new LinkedCaseInsensitiveMap<>();
	}

	public RowDocument(int expectedSize) {
		this.delegate = new LinkedCaseInsensitiveMap<>(expectedSize);
	}

	public RowDocument(Map<String, ? extends Object> map) {

		this.delegate = new LinkedCaseInsensitiveMap<>();
		this.delegate.putAll(map);
	}

	/**
	 * Factory method to create a RowDocument from a field and value.
	 *
	 * @param field the file name to use.
	 * @param value the value to use, can be {@literal null}.
	 * @return
	 */
	public static RowDocument of(String field, @Nullable Object value) {
		return new RowDocument().append(field, value);
	}

	/**
	 * Retrieve the value at {@code key} as {@link List}.
	 *
	 * @param key
	 * @return the value or {@literal null}.
	 * @throws ClassCastException if {@code key} holds a value that is not a {@link List}.
	 */
	@Nullable
	public List getList(String key) {

		Object item = get(key);
		if (item instanceof List<?> || item == null) {
			return (List) item;
		}

		throw new ClassCastException(String.format("Cannot cast element %s be cast to List", item));
	}

	/**
	 * Retrieve the value at {@code key} as {@link Map}.
	 *
	 * @param key
	 * @return the value or {@literal null}.
	 * @throws ClassCastException if {@code key} holds a value that is not a {@link Map}.
	 */
	@Nullable
	public Map<String, Object> getMap(String key) {

		Object item = get(key);
		if (item instanceof Map || item == null) {
			return (Map<String, Object>) item;
		}

		throw new ClassCastException(String.format("Cannot cast element %s be cast to Map", item));
	}

	/**
	 * Retrieve the value at {@code key} as {@link RowDocument}.
	 *
	 * @param key
	 * @return the value or {@literal null}.
	 * @throws ClassCastException if {@code key} holds a value that is not a {@link RowDocument}.
	 */
	public RowDocument getDocument(String key) {

		Object item = get(key);
		if (item instanceof RowDocument || item == null) {
			return (RowDocument) item;
		}

		throw new ClassCastException(String.format("Cannot cast element %s be cast to RowDocument", item));
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return delegate.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return delegate.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return delegate.get(key);
	}

	@Nullable
	@Override
	public Object put(String key, @Nullable Object value) {

		Assert.notNull(key, "Key must not be null!");

		return delegate.put(key, value);
	}

	/**
	 * Appends a new entry (or overwrites an existing value at {@code key}).
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public RowDocument append(String key, @Nullable Object value) {

		put(key, value);
		return this;
	}

	@Override
	public Object remove(Object key) {
		return delegate.remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ?> m) {
		delegate.putAll(m);
	}

	@Override
	public void clear() {
		delegate.clear();
	}

	@Override
	public Set<String> keySet() {
		return delegate.keySet();
	}

	@Override
	public Collection<Object> values() {
		return delegate.values();
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return delegate.entrySet();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		RowDocument that = (RowDocument) o;

		return ObjectUtils.nullSafeEquals(delegate, that.delegate);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(delegate);
	}

	@Override
	public Object getOrDefault(Object key, Object defaultValue) {
		return delegate.getOrDefault(key, defaultValue);
	}

	@Override
	public void forEach(BiConsumer<? super String, ? super Object> action) {
		delegate.forEach(action);
	}

	@Override
	public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
		delegate.replaceAll(function);
	}

	@Nullable
	@Override
	public Object putIfAbsent(String key, Object value) {
		return delegate.putIfAbsent(key, value);
	}

	@Override
	public boolean remove(Object key, Object value) {
		return delegate.remove(key, value);
	}

	@Override
	public boolean replace(String key, Object oldValue, Object newValue) {
		return delegate.replace(key, oldValue, newValue);
	}

	@Nullable
	@Override
	public Object replace(String key, Object value) {
		return delegate.replace(key, value);
	}

	@Override
	public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
		return delegate.computeIfAbsent(key, mappingFunction);
	}

	@Override
	public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
		return delegate.computeIfPresent(key, remappingFunction);
	}

	@Override
	public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
		return delegate.compute(key, remappingFunction);
	}

	@Override
	public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
		return delegate.merge(key, value, remappingFunction);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + delegate.toString();
	}

}
