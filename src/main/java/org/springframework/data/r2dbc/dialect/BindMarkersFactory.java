package org.springframework.data.r2dbc.dialect;

import org.springframework.util.Assert;

/**
 * This class creates new {@link BindMarkers} instances to bind parameter for a specific {@link io.r2dbc.spi.Statement}.
 *
 * @author Mark Paluch
 * @see BindMarkers
 * @see io.r2dbc.spi.Statement
 */
@FunctionalInterface
public interface BindMarkersFactory {

	/**
	 * Create a new {@link BindMarkers} instance.
	 *
	 * @return a new {@link BindMarkers} instance.
	 */
	BindMarkers create();

	/**
	 * Create index-based {@link BindMarkers}.
	 *
	 * @param prefix bind parameter prefix.
	 * @param beginWith the first index to use.
	 * @return a {@link BindMarkersFactory} using {@code prefix} and {@code beginWith}.
	 */
	static BindMarkersFactory indexed(String prefix, int beginWith) {

		Assert.notNull(prefix, "Prefix must not be null!");
		return () -> new IndexedBindMarkers(prefix, beginWith);
	}

	/**
	 * Create named {@link BindMarkers}. Named bind markers can support {@link BindMarkers#next(String) name hints}.
	 * Typically, named markers use name hints. If no namehint is given, named bind markers use a counter to generate
	 * unique bind markers.
	 *
	 * @param prefix bind parameter prefix.
	 * @param indexPrefix prefix for bind markers that were created by incrementing a counter to generate a unique bind
	 *          marker.
	 * @param nameLimit maximal length of parameter names when using name hints.
	 * @return a {@link BindMarkersFactory} using {@code prefix} and {@code beginWith}.
	 */
	static BindMarkersFactory named(String prefix, String indexPrefix, int nameLimit) {

		Assert.notNull(prefix, "Prefix must not be null!");
		Assert.notNull(indexPrefix, "Index prefix must not be null!");

		return () -> new NamedBindMarkers(prefix, indexPrefix, nameLimit);
	}
}
