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
package org.springframework.data.relational.core.conversion;

import java.util.Objects;

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Wrapper value object for a {@link RowDocument} to be able to access raw values by
 * {@link RelationalPersistentProperty} references. The accessors will transparently resolve nested document values that
 * a {@link RelationalPersistentProperty} might refer to through a path expression in field names.
 *
 * @author Mark Paluch
 * @author Chanhyeong Cho
 * @since 3.2
 */
public class RowDocumentAccessor {

	private final RowDocument document;

	/**
	 * Creates a new {@link RowDocumentAccessor} for the given {@link RowDocument}.
	 *
	 * @param document must be a {@link RowDocument} effectively, must not be {@literal null}.
	 */
	RowDocumentAccessor(RowDocument document) {

		Assert.notNull(document, "Document must not be null");
		this.document = document;
	}

	/**
	 * @return the underlying {@link RowDocument document}.
	 */
	public RowDocument getDocument() {
		return this.document;
	}

	/**
	 * Copies all mappings from the given {@link RowDocument} to the underlying target {@link RowDocument}. These mappings
	 * will replace any mappings that the target document had for any of the keys currently in the specified map.
	 *
	 * @param source
	 */
	public void putAll(RowDocument source) {
		document.putAll(source);
	}

	/**
	 * Puts the given value into the backing {@link RowDocument} based on the coordinates defined through the given
	 * {@link RelationalPersistentProperty}. By default, this will be the plain field name. But field names might also
	 * consist of path traversals so we might need to create intermediate {@link RowDocument}s.
	 *
	 * @param prop must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public void put(RelationalPersistentProperty prop, @Nullable Object value) {

		Assert.notNull(prop, "RelationalPersistentProperty must not be null");
		String fieldName = getColumnName(prop);

		document.put(fieldName, value);
	}

	/**
	 * Returns the value the given {@link RelationalPersistentProperty} refers to. By default, this will be a direct field
	 * but the method will also transparently resolve nested values the {@link RelationalPersistentProperty} might refer
	 * to through a path expression in the field name metadata.
	 *
	 * @param property must not be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Object get(RelationalPersistentProperty property) {
		return document.get(getColumnName(property));
	}

	/**
	 * Returns whether the underlying {@link RowDocument} has a value ({@literal null} or non-{@literal null}) for the
	 * given {@link RelationalPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @return {@literal true} if no non {@literal null} value present.
	 */
	public boolean hasValue(RelationalPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		return document.get(getColumnName(property)) != null;
	}

	String getColumnName(RelationalPersistentProperty prop) {
		return prop.getColumnName().getReference();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (RowDocumentAccessor) obj;
		return Objects.equals(this.document, that.document);
	}

	@Override
	public int hashCode() {
		return Objects.hash(document);
	}

	@Override
	public String toString() {
		return "RowDocumentAccessor[" + "document=" + document + ']';
	}
}
