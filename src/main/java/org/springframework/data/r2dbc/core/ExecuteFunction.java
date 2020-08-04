/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

import java.util.function.BiFunction;

import org.reactivestreams.Publisher;

/**
 * Represents a function that executes a {@link io.r2dbc.spi.Statement} for a (delayed) {@link io.r2dbc.spi.Result}
 * stream.
 * <p>
 * Note that discarded {@link Result} objects must be consumed according to the R2DBC spec via either
 * {@link Result#getRowsUpdated()} or {@link Result#map(BiFunction)}.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see Statement#execute()
 * @deprecated since 1.2, use Spring's {@link org.springframework.r2dbc.core.ExecuteFunction} support instead.
 */
@Deprecated
@FunctionalInterface
public interface ExecuteFunction extends org.springframework.r2dbc.core.ExecuteFunction {

	/**
	 * Execute the given {@link Statement} for a stream of {@link Result}s.
	 *
	 * @param statement the request to execute.
	 * @return the delayed result stream.
	 */
	Publisher<? extends Result> execute(Statement statement);
}
