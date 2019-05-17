package org.springframework.data.r2dbc.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.JodaTimeConverters;
import org.springframework.data.r2dbc.mapping.R2dbcSimpleTypeHolder;

/**
 * Value object to capture custom conversion. {@link R2dbcCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @see CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 */
public class R2dbcCustomConversions extends CustomConversions {

	public static final List<Object> STORE_CONVERTERS;

	private static final StoreConversions STORE_CONVERSIONS;

	static {

		List<Object> converters = new ArrayList<>();

		converters.addAll(R2dbcConverters.getConvertersToRegister());
		converters.addAll(JodaTimeConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(R2dbcSimpleTypeHolder.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Creates a new {@link R2dbcCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public R2dbcCustomConversions(Collection<?> converters) {
		super(STORE_CONVERSIONS, appendOverrides(converters));
	}

	/**
	 * Creates a new {@link R2dbcCustomConversions} instance registering the given converters.
	 *
	 * @param storeConversions must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 */
	public R2dbcCustomConversions(StoreConversions storeConversions, Collection<?> converters) {
		super(storeConversions, appendOverrides(converters));
	}

	private static Collection<?> appendOverrides(Collection<?> converters) {

		List<Object> objects = new ArrayList<>(converters);
		objects.addAll(R2dbcConverters.getOverrideConvertersToRegister());

		return objects;
	}
}
