/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.core.query;

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a value function to return arbitrary values that can be escaped before returning the actual value. Can be
 * used with the criteria API for deferred value retrieval.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see Escaper
 * @see Supplier
 */
@FunctionalInterface
public interface ValueFunction<T> extends Function<Escaper, T> {

	/**
	 * Produces a value by considering the given {@link Escaper}.
	 *
	 * @param escaper the escaper to use.
	 * @return the return value, may be {@literal null}.
	 */
	@Nullable
	@Override
	T apply(Escaper escaper);

	/**
	 * Adapts this value factory into a {@link Supplier} by using the given {@link Escaper}.
	 *
	 * @param escaper the escaper to use.
	 * @return the value factory
	 */
	default Supplier<T> toSupplier(Escaper escaper) {

		Assert.notNull(escaper, "Escaper must not be null");

		return () -> apply(escaper);
	}

	/**
	 * Return a new ValueFunction applying the given mapping {@link Function}. The mapping function is applied after
	 * applying {@link Escaper}.
	 *
	 * @param mapper the mapping function to apply to the value.
	 * @param <R> the type of the value returned from the mapping function.
	 * @return a new {@literal ValueFunction}.
	 * @since 3.2
	 */
	default <R> ValueFunction<R> map(Function<T, R> mapper) {

		Assert.notNull(mapper, "Mapping function must not be null");

		return escaper -> mapper.apply(this.apply(escaper));
	}
}
