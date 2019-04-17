/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Anonymous, index-based bind marker using a static placeholder. Instances are bound by the ordinal position ordered by
 * the appearance of the placeholder. This implementation creates indexed bind markers using an anonymous placeholder
 * that correlates with an index.
 * <p>
 *     Note: Anonymous bind markers are problematic because the have to appear in generated SQL in the same order they get generated.
 *
 *     This might cause challenges in the future with complex generate statements.
 *     For example those containing subselects which limit the freedom of arranging bind markers.
 * </p>
 *
 * @author Mark Paluch
 */
class AnonymousBindMarkers implements BindMarkers {

	private static final AtomicIntegerFieldUpdater<AnonymousBindMarkers> COUNTER_INCREMENTER = AtomicIntegerFieldUpdater
			.newUpdater(AnonymousBindMarkers.class, "counter");

	// access via COUNTER_INCREMENTER
	@SuppressWarnings("unused") private volatile int counter;

	private final String placeholder;

	/**
	 * Creates a new {@link AnonymousBindMarkers} instance given {@code placeholder}.
	 *
	 * @param placeholder parameter bind marker.
	 */
	AnonymousBindMarkers(String placeholder) {
		this.counter = 0;
		this.placeholder = placeholder;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarkers#next()
	 */
	@Override
	public BindMarker next() {

		int index = COUNTER_INCREMENTER.getAndIncrement(this);

		return new IndexedBindMarker(placeholder, index);
	}

}
