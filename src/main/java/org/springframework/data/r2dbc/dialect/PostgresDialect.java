package org.springframework.data.r2dbc.dialect;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.util.Lazy;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.util.ClassUtils;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 */
public class PostgresDialect extends org.springframework.data.relational.core.dialect.PostgresDialect
		implements R2dbcDialect {

	private static final Set<Class<?>> SIMPLE_TYPES;

	static {

		Set<Class<?>> simpleTypes = new HashSet<>(Arrays.asList(UUID.class, URL.class, URI.class, InetAddress.class));

		if (ClassUtils.isPresent("io.r2dbc.postgresql.codec.Json", PostgresDialect.class.getClassLoader())) {

			simpleTypes
					.add(ClassUtils.resolveClassName("io.r2dbc.postgresql.codec.Json", PostgresDialect.class.getClassLoader()));
		}

		SIMPLE_TYPES = simpleTypes;
	}

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);

	private final Lazy<ArrayColumns> arrayColumns = Lazy.of(() -> new R2dbcArrayColumns(
			org.springframework.data.relational.core.dialect.PostgresDialect.INSTANCE.getArraySupport(),
			getSimpleTypeHolder()));

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getBindMarkersFactory()
	 */
	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getSimpleTypesKeys()
	 */
	@Override
	public Collection<? extends Class<?>> getSimpleTypes() {
		return SIMPLE_TYPES;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return this.arrayColumns.get();
	}

	private static class R2dbcArrayColumns implements ArrayColumns {

		private final ArrayColumns delegate;
		private final SimpleTypeHolder simpleTypeHolder;

		R2dbcArrayColumns(ArrayColumns delegate, SimpleTypeHolder simpleTypeHolder) {
			this.delegate = delegate;
			this.simpleTypeHolder = simpleTypeHolder;
		}

		@Override
		public boolean isSupported() {
			return this.delegate.isSupported();
		}

		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Class<?> typeToUse = userType;
			while (typeToUse.getComponentType() != null) {
				typeToUse = typeToUse.getComponentType();
			}

			if (!this.simpleTypeHolder.isSimpleType(typeToUse)) {
				throw new IllegalArgumentException("Unsupported array type: " + ClassUtils.getQualifiedName(typeToUse));
			}

			return this.delegate.getArrayType(typeToUse);
		}
	}

}
