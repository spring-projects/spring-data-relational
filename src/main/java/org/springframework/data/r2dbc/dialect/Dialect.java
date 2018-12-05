package org.springframework.data.r2dbc.dialect;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.r2dbc.dialect.ArrayColumns.Unsupported;

/**
 * Represents a dialect that is implemented by a particular database.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public interface Dialect {

	/**
	 * Returns the {@link BindMarkersFactory} used by this dialect.
	 *
	 * @return the {@link BindMarkersFactory} used by this dialect.
	 */
	BindMarkersFactory getBindMarkersFactory();

	/**
	 * Returns the clause to include for returning generated keys. The returned query is directly appended to
	 * {@code INSERT} statements.
	 *
	 * @return the clause to include for returning generated keys.
	 * @deprecated to be removed after upgrading to R2DBC 1.0M7 in favor of using the driver's direct support for
	 *             retrieving generated keys.
	 */
	@Deprecated
	String generatedKeysClause();

	/**
	 * Return a collection of types that are natively supported by this database/driver. Defaults to
	 * {@link Collections#emptySet()}.
	 *
	 * @return a collection of types that are natively supported by this database/driver. Defaults to
	 *         {@link Collections#emptySet()}.
	 */
	default Collection<? extends Class<?>> getSimpleTypes() {
		return Collections.emptySet();
	}

	/**
	 * Return the {@link SimpleTypeHolder} for this dialect.
	 *
	 * @return the {@link SimpleTypeHolder} for this dialect.
	 * @see #getSimpleTypes()
	 */
	default SimpleTypeHolder getSimpleTypeHolder() {
		return new SimpleTypeHolder(new HashSet<>(getSimpleTypes()), true);
	}

	/**
	 * Return the {@link LimitClause} used by this dialect.
	 *
	 * @return the {@link LimitClause} used by this dialect.
	 */
	LimitClause limit();

	/**
	 * Returns the array support object that describes how array-typed columns are supported by this dialect.
	 *
	 * @return the array support object that describes how array-typed columns are supported by this dialect.
	 */
	default ArrayColumns getArraySupport() {
		return Unsupported.INSTANCE;
	}
}
