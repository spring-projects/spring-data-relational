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
package org.springframework.data.jdbc.core.convert;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;

/**
 * {@link Collector} which invokes functions on the elements of a {@link java.util.stream.Stream} containing
 * {@link DataAccessStrategy}s until one function completes without throwing an exception. If all invocations throw
 * exceptions this {@link Collector} throws itself an exception, gathering all exceptions thrown.
 *
 * @author Jens Schauder
 */
class FunctionCollector<T> implements Collector<DataAccessStrategy, FunctionCollector.ResultOrException<T>, T> {

	private final Function<DataAccessStrategy, T> method;

	FunctionCollector(Function<DataAccessStrategy, T> method) {
		this.method = method;
	}

	@Override
	public Supplier<ResultOrException<T>> supplier() {
		return ResultOrException::new;
	}

	@Override
	public BiConsumer<ResultOrException<T>, DataAccessStrategy> accumulator() {

		return (roe, das) -> {

			if (!roe.hasResult()) {

				try {
					roe.setResult(method.apply(das));
				} catch (Exception ex) {
					roe.add(ex);
				}
			}
		};
	}

	@Override
	public BinaryOperator<ResultOrException<T>> combiner() {

		return (roe1, roe2) -> {
			throw new UnsupportedOperationException("Can't combine method calls");
		};
	}

	@Override
	public Function<ResultOrException<T>, T> finisher() {

		return roe -> {

			if (roe.hasResult)
				return roe.result;
			else
				throw new CombinedDataAccessException("Failed to perform data access with all available strategies",
						Collections.unmodifiableList(roe.exceptions));
		};
	}

	@Override
	public Set<Characteristics> characteristics() {
		return Collections.emptySet();
	}

	/**
	 * Stores intermediate results. I.e. a list of exceptions caught so far, any actual result and the fact, if there
	 * actually is an result.
	 */
	static class ResultOrException<T> {

		private T result;
		private final List<Exception> exceptions = new LinkedList<>();
		private boolean hasResult = false;

		private boolean hasResult() {
			return hasResult;
		}

		private void setResult(T result) {
			this.result = result;
			hasResult = true;
		}

		public void add(Exception ex) {
			exceptions.add(ex);
		}
	}

	static class CombinedDataAccessException extends DataAccessException {

		CombinedDataAccessException(String message, List<Exception> exceptions) {
			super(combineMessage(message, exceptions), exceptions.get(exceptions.size() - 1));
		}

		private static String combineMessage(String message, List<Exception> exceptions) {
			return message + exceptions.stream().map(Exception::getMessage).collect(Collectors.joining("\n\t", "\n\t", ""));
		}
	}
}
