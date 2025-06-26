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
import java.util.function.Function;

import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.util.Assert;

/**
 * Name-based bind markers.
 *
 * @author Mark Paluch
 */
class NamedBindMarkers implements BindMarkers {

	private static final AtomicIntegerFieldUpdater<NamedBindMarkers> COUNTER_INCREMENTER = AtomicIntegerFieldUpdater
			.newUpdater(NamedBindMarkers.class, "counter");

	private final String prefix;

	private final String namePrefix;

	private final int nameLimit;

	private final Function<String, String> hintFilterFunction;

	// access via COUNTER_INCREMENTER
	@SuppressWarnings("unused") private volatile int counter;

	NamedBindMarkers(String prefix, String namePrefix, int nameLimit, Function<String, String> hintFilterFunction) {
		this.prefix = prefix;
		this.namePrefix = namePrefix;
		this.nameLimit = nameLimit;
		this.hintFilterFunction = hintFilterFunction;
	}

	@Override
	public BindMarker next() {
		String name = nextName();
		return BindMarker.named(this.prefix + name);
	}

	@Override
	public BindMarker next(String hint) {
		Assert.notNull(hint, "Name hint must not be null");
		String name = nextName() + this.hintFilterFunction.apply(hint);

		if (name.length() > this.nameLimit) {
			name = name.substring(0, this.nameLimit);
		}

		return BindMarker.named(this.prefix + name);
	}

	private String nextName() {
		int index = COUNTER_INCREMENTER.getAndIncrement(this);
		return this.namePrefix + index;
	}

}
