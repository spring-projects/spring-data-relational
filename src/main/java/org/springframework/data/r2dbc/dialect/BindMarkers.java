package org.springframework.data.r2dbc.dialect;

/**
 * Bind markers represent placeholders in SQL queries for substitution for an actual parameter. Using bind markers
 * allows creating safe queries so query strings are not required to contain escaped values but rather the driver
 * encodes parameter in the appropriate representation.
 * <p/>
 * {@link BindMarkers} is stateful and can be only used for a single binding pass of one or more parameters. It
 * maintains bind indexes/bind parameter names.
 *
 * @author Mark Paluch
 * @see BindMarker
 * @see BindMarkersFactory
 * @see io.r2dbc.spi.Statement#bind
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.core.binding} instead.
 */
@FunctionalInterface
@Deprecated
public interface BindMarkers extends org.springframework.r2dbc.core.binding.BindMarkers {

	/**
	 * Creates a new {@link BindMarker}.
	 *
	 * @return a new {@link BindMarker}.
	 */
	BindMarker next();

	/**
	 * Creates a new {@link BindMarker} that accepts a {@code hint}. Implementations are allowed to consider/ignore/filter
	 * the name hint to create more expressive bind markers.
	 *
	 * @param hint an optional name hint that can be used as part of the bind marker.
	 * @return a new {@link BindMarker}.
	 */
	default BindMarker next(String hint) {
		return next();
	}
}
