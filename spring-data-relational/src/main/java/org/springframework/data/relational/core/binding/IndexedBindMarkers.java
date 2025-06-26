/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.data.relational.core.binding;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.springframework.data.relational.core.sql.BindMarker;

/**
 * Index-based bind markers. This implementation creates indexed bind markers using a numeric index and an optional
 * prefix for bind markers to be represented within the query string.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class IndexedBindMarkers implements BindMarkers {

	private static final AtomicIntegerFieldUpdater<IndexedBindMarkers> COUNTER_INCREMENTER = AtomicIntegerFieldUpdater
			.newUpdater(IndexedBindMarkers.class, "counter");

	private final int offset;

	private final String prefix;

	// access via COUNTER_INCREMENTER
	@SuppressWarnings("unused") private volatile int counter;

	/**
	 * Create a new indexed instance for the given {@code prefix} and {@code beginWith} value.
	 *
	 * @param prefix the bind parameter prefix
	 * @param beginIndex the first index to use
	 */
	IndexedBindMarkers(String prefix, int beginIndex) {
		this.counter = 0;
		this.prefix = prefix;
		this.offset = beginIndex;
	}

	@Override
	public BindMarker next() {
		int index = COUNTER_INCREMENTER.getAndIncrement(this);
		return BindMarker.indexed(this.prefix + "" + (index + this.offset), index);
	}

}
