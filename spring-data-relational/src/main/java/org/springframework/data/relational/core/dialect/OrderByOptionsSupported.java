/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.util.StringJoiner;

import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;

/**
 * This enum represents the different sets of {@code ORDER BY} options that are supported by different {@link Dialect}s.
 *
 * @author Chirag Tailor
 */
public enum OrderByOptionsSupported implements OrderByOptionsSupport {
	NULL_HANDLING(true),
	DEFAULT(false);

	private final boolean supportNullHandling;

	OrderByOptionsSupported(boolean supportNullHandling) {
		this.supportNullHandling = supportNullHandling;
	}

	@Override
	public String resolve(@Nullable Sort.Direction direction, Sort.NullHandling nullHandling) {

		StringJoiner stringJoiner = new StringJoiner(" ");
		if (direction != null) {
			stringJoiner.add(direction.toString());
		}
		if (supportNullHandling && !Sort.NullHandling.NATIVE.equals(nullHandling)) {
			stringJoiner.add(nullHandling.toString().replace("_", " "));
		}
		return stringJoiner.toString();
	}
}
