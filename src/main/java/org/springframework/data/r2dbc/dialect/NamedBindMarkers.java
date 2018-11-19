package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.Statement;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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

	private final String indexPrefix;

	private final int nameLimit;

	NamedBindMarkers(String prefix, String indexPrefix, int nameLimit) {

		this.prefix = prefix;
		this.indexPrefix = indexPrefix;
		this.nameLimit = nameLimit;
	}

	@Override
	public BindMarker next() {

		String name = nextName();

		return new NamedBindMarker(prefix + name, name);
	}

	@Override
	public BindMarker next(String nameHint) {

		Assert.notNull(nameHint, "Name hint must not be null");

		String name = nextName();

		String filteredNameHint = filter(nameHint);

		if (!filteredNameHint.isEmpty()) {
			name += "_" + filteredNameHint;
		}

		if (name.length() > nameLimit) {
			name = name.substring(0, nameLimit);
		}

		return new NamedBindMarker(prefix + name, name);
	}

	private String nextName() {

		int index = COUNTER_INCREMENTER.getAndIncrement(this);
		return indexPrefix + index;
	}

	private static String filter(CharSequence input) {

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < input.length(); i++) {

			char ch = input.charAt(i);

			// ascii letter or digit
			if (Character.isLetterOrDigit(ch) && ch < 127) {
				builder.append(ch);
			}

		}

		return builder.toString();
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
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindValue(io.r2dbc.spi.Statement, java.lang.Object)
		 */
		@Override
		public void bindValue(Statement<?> statement, Object value) {
			statement.bind(this.identifier, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindNull(io.r2dbc.spi.Statement, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, Class<?> valueType) {
			statement.bindNull(this.identifier, valueType);
		}
	}
}
