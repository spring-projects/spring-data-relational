package org.springframework.data.r2dbc.dialect;

import java.util.function.Function;

import org.springframework.util.Assert;

/**
 * This class creates new {@link BindMarkers} instances to bind parameter for a specific {@link io.r2dbc.spi.Statement}.
 * <p/>
 * Bind markers can be typically represented as placeholder and identifier. Placeholders are used within the query to
 * execute so the underlying database system can substitute the placeholder with the actual value. Identifiers are used
 * in R2DBC drivers to bind a value to a bind marker. Identifiers are typically a part of an entire bind marker when
 * using indexed or named bind markers.
 *
 * @author Mark Paluch
 * @see BindMarkers
 * @see io.r2dbc.spi.Statement
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.core.binding} instead.
 */
@FunctionalInterface
@Deprecated
public interface BindMarkersFactory extends org.springframework.r2dbc.core.binding.BindMarkersFactory {

	/**
	 * Create a new {@link BindMarkers} instance.
	 *
	 * @return a new {@link BindMarkers} instance.
	 */
	BindMarkers create();

	/**
	 * Return whether the {@link BindMarkersFactory} uses identifiable placeholders.
	 *
	 * @return whether the {@link BindMarkersFactory} uses identifiable placeholders. {@literal false} if multiple
	 *         placeholders cannot be distinguished by just the {@link BindMarker#getPlaceholder() placeholder}
	 *         identifier.
	 */
	default boolean identifiablePlaceholders() {
		return true;
	}

	/**
	 * Create index-based {@link BindMarkers} using indexes to bind parameters. Allow customization of the bind marker
	 * placeholder {@code prefix} to represent the bind marker as placeholder within the query.
	 *
	 * @param prefix bind parameter prefix that is included in {@link BindMarker#getPlaceholder()} but not the actual
	 *          identifier.
	 * @param beginWith the first index to use.
	 * @return a {@link BindMarkersFactory} using {@code prefix} and {@code beginWith}.
	 * @see io.r2dbc.spi.Statement#bindNull(int, Class)
	 * @see io.r2dbc.spi.Statement#bind(int, Object)
	 */
	static BindMarkersFactory indexed(String prefix, int beginWith) {

		Assert.notNull(prefix, "Prefix must not be null!");

		org.springframework.r2dbc.core.binding.BindMarkersFactory factory = org.springframework.r2dbc.core.binding.BindMarkersFactory
				.indexed(prefix, beginWith);

		return new BindMarkersFactory() {

			@Override
			public BindMarkers create() {
				return new BindMarkersAdapter(factory.create());
			}

			@Override
			public boolean identifiablePlaceholders() {
				return factory.identifiablePlaceholders();
			}
		};
	}

	/**
	 * Creates anonymous, index-based bind marker using a static placeholder. Instances are bound by the ordinal position
	 * ordered by the appearance of the placeholder. This implementation creates indexed bind markers using an anonymous
	 * placeholder that correlates with an index.
	 *
	 * @param placeholder parameter placeholder.
	 * @return a {@link BindMarkersFactory} using {@code placeholder}.
	 * @see io.r2dbc.spi.Statement#bindNull(int, Class)
	 * @see io.r2dbc.spi.Statement#bind(int, Object)
	 */
	static BindMarkersFactory anonymous(String placeholder) {

		Assert.hasText(placeholder, "Placeholder must not be empty!");
		org.springframework.r2dbc.core.binding.BindMarkersFactory factory = org.springframework.r2dbc.core.binding.BindMarkersFactory
				.anonymous(placeholder);

		return new BindMarkersFactory() {

			@Override
			public BindMarkers create() {
				return new BindMarkersAdapter(factory.create());
			}

			@Override
			public boolean identifiablePlaceholders() {
				return factory.identifiablePlaceholders();
			}
		};
	}

	/**
	 * Create named {@link BindMarkers} using identifiers to bind parameters. Named bind markers can support
	 * {@link BindMarkers#next(String) name hints}. If no {@link BindMarkers#next(String) hint} is given, named bind
	 * markers can use a counter or a random value source to generate unique bind markers.
	 * <p/>
	 * Allow customization of the bind marker placeholder {@code prefix} and {@code namePrefix} to represent the bind
	 * marker as placeholder within the query.
	 *
	 * @param prefix bind parameter prefix that is included in {@link BindMarker#getPlaceholder()} but not the actual
	 *          identifier.
	 * @param namePrefix prefix for bind marker name that is included in {@link BindMarker#getPlaceholder()} and the
	 *          actual identifier.
	 * @param maxLength maximal length of parameter names when using name hints.
	 * @return a {@link BindMarkersFactory} using {@code prefix} and {@code beginWith}.
	 * @see io.r2dbc.spi.Statement#bindNull(String, Class)
	 * @see io.r2dbc.spi.Statement#bind(String, Object)
	 */
	static BindMarkersFactory named(String prefix, String namePrefix, int maxLength) {
		return named(prefix, namePrefix, maxLength, Function.identity());
	}

	/**
	 * Create named {@link BindMarkers} using identifiers to bind parameters. Named bind markers can support
	 * {@link BindMarkers#next(String) name hints}. If no {@link BindMarkers#next(String) hint} is given, named bind
	 * markers can use a counter or a random value source to generate unique bind markers.
	 *
	 * @param prefix bind parameter prefix that is included in {@link BindMarker#getPlaceholder()} but not the actual
	 *          identifier.
	 * @param namePrefix prefix for bind marker name that is included in {@link BindMarker#getPlaceholder()} and the
	 *          actual identifier.
	 * @param maxLength maximal length of parameter names when using name hints.
	 * @param hintFilterFunction filter {@link Function} to consider database-specific limitations in bind marker/variable
	 *          names such as ASCII chars only.
	 * @return a {@link BindMarkersFactory} using {@code prefix} and {@code beginWith}.
	 * @see io.r2dbc.spi.Statement#bindNull(String, Class)
	 * @see io.r2dbc.spi.Statement#bind(String, Object)
	 */
	static BindMarkersFactory named(String prefix, String namePrefix, int maxLength,
			Function<String, String> hintFilterFunction) {

		Assert.notNull(prefix, "Prefix must not be null!");
		Assert.notNull(namePrefix, "Index prefix must not be null!");
		Assert.notNull(hintFilterFunction, "Hint filter function must not be null!");

		org.springframework.r2dbc.core.binding.BindMarkersFactory factory = org.springframework.r2dbc.core.binding.BindMarkersFactory
				.named(prefix, namePrefix, maxLength, hintFilterFunction);

		return new BindMarkersFactory() {

			@Override
			public BindMarkers create() {
				return new BindMarkersAdapter(factory.create());
			}

			@Override
			public boolean identifiablePlaceholders() {
				return factory.identifiablePlaceholders();
			}
		};
	}
}
