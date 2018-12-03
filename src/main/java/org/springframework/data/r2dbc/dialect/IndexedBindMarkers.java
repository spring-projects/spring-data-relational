package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.Statement;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Index-based bind marker. This implementation creates indexed bind markers using a numeric index and an optional
 * prefix for bind markers to be represented within the query string.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class IndexedBindMarkers implements BindMarkers {

	private static final AtomicIntegerFieldUpdater<org.springframework.data.r2dbc.dialect.IndexedBindMarkers> COUNTER_INCREMENTER = AtomicIntegerFieldUpdater
			.newUpdater(org.springframework.data.r2dbc.dialect.IndexedBindMarkers.class, "counter");

	// access via COUNTER_INCREMENTER
	@SuppressWarnings("unused") private volatile int counter;

	private final int offset;
	private final String prefix;

	/**
	 * Creates a new {@link IndexedBindMarker} instance given {@code prefix} and {@code beginWith}.
	 *
	 * @param prefix bind parameter prefix.
	 * @param beginWith the first index to use.
	 */
	IndexedBindMarkers(String prefix, int beginWith) {
		this.counter = 0;
		this.prefix = prefix;
		this.offset = beginWith;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarkers#next()
	 */
	@Override
	public BindMarker next() {

		int index = COUNTER_INCREMENTER.getAndIncrement(this);

		return new IndexedBindMarker(prefix + "" + (index + offset), index);
	}

	/**
	 * A single indexed bind marker.
	 */
	static class IndexedBindMarker implements BindMarker {

		private final String placeholder;

		private int index;

		IndexedBindMarker(String placeholder, int index) {
			this.placeholder = placeholder;
			this.index = index;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#getPlaceholder()
		 */
		@Override
		public String getPlaceholder() {
			return placeholder;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindValue(io.r2dbc.spi.Statement, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, Object value) {
			statement.bind(this.index, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindNull(io.r2dbc.spi.Statement, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, Class<?> valueType) {
			statement.bindNull(this.index, valueType);
		}
	}
}
