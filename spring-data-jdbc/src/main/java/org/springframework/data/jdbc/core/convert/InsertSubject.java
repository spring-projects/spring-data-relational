/*
 * Copyright 2022-2024 the original author or authors.
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

import java.util.Objects;

import org.springframework.lang.Nullable;

/**
 * The subject of an insert, described by the entity instance and its {@link Identifier}, where identifier contains
 * information about data that needs to be considered for the insert but which is not part of the entity. Namely
 * references back to a parent entity and key/index columns for entities that are stored in a {@link java.util.Map} or
 * {@link java.util.List}.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
public final class InsertSubject<T> {

	private final T instance;
	private final Identifier identifier;

	public static <T> InsertSubject<T> describedBy(T instance, Identifier identifier) {
		return new InsertSubject<>(instance, identifier);
	}

	private InsertSubject(T instance, Identifier identifier) {

		this.instance = instance;
		this.identifier = identifier;
	}

	public T getInstance() {
		return instance;
	}

	public Identifier getIdentifier() {
		return identifier;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		InsertSubject<?> that = (InsertSubject<?>) o;
		return Objects.equals(instance, that.instance) && Objects.equals(identifier, that.identifier);
	}

	@Override
	public int hashCode() {
		return Objects.hash(instance, identifier);
	}

	@Override
	public String toString() {
		return "InsertSubject{" + "instance=" + instance + ", identifier=" + identifier + '}';
	}
}
