package org.springframework.data.r2dbc.function.convert;

import java.util.Collection;

import org.springframework.data.convert.CustomConversions;

/**
 * Value object to capture custom conversion. {@link R2dbcCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @see CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 */
public class R2dbcCustomConversions extends CustomConversions {

	/**
	 * Creates a new {@link CustomConversions} instance registering the given converters.
	 *
	 * @param storeConversions must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 */
	public R2dbcCustomConversions(StoreConversions storeConversions, Collection<?> converters) {
		super(storeConversions, converters);
	}
}
