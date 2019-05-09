package org.springframework.data.r2dbc.dialect;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import org.springframework.util.Assert;

/**
 * Name-based bind markers.
 *
 * @author Mark Paluch
 */
class NamedBindMarkers implements BindMarkers {

	private static final AtomicIntegerFieldUpdater<NamedBindMarkers> COUNTER_INCREMENTER = AtomicIntegerFieldUpdater
			.newUpdater(NamedBindMarkers.class, "counter");

	// access via COUNTER_INCREMENTER
	@SuppressWarnings("unused") private volatile int counter;

	private final String prefix;

	private final String namePrefix;

	private final int nameLimit;

	private final Function<String, String> hintFilterFunction;

	NamedBindMarkers(String prefix, String namePrefix, int nameLimit, Function<String, String> hintFilterFunction) {

		this.prefix = prefix;
		this.namePrefix = namePrefix;
		this.nameLimit = nameLimit;
		this.hintFilterFunction = hintFilterFunction;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarkers#next()
	 */
	@Override
	public BindMarker next() {

		String name = nextName();

		return new NamedBindMarker(prefix + name, name);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarkers#next(java.lang.String)
	 */
	@Override
	public BindMarker next(String hint) {

		Assert.notNull(hint, "Name hint must not be null");

		String name = nextName() + hintFilterFunction.apply(hint);

		if (name.length() > nameLimit) {
			name = name.substring(0, nameLimit);
		}

		return new NamedBindMarker(prefix + name, name);
	}

	private String nextName() {

		int index = COUNTER_INCREMENTER.getAndIncrement(this);
		return namePrefix + index;
	}

	/**
	 * A single named bind marker.
	 */
	static class NamedBindMarker implements BindMarker {

		private final String placeholder;

		private final String identifier;

		NamedBindMarker(String placeholder, String identifier) {

			this.placeholder = placeholder;
			this.identifier = identifier;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#getPlaceholder()
		 */
		@Override
		public String getPlaceholder() {
			return this.placeholder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindValue(org.springframework.data.r2dbc.dialect.BindTarget, java.lang.Object)
		 */
		@Override
		public void bind(BindTarget target, Object value) {
			target.bind(this.identifier, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindNull(org.springframework.data.r2dbc.dialect.BindTarget, java.lang.Class)
		 */
		@Override
		public void bindNull(BindTarget target, Class<?> valueType) {
			target.bindNull(this.identifier, valueType);
		}
	}
}
