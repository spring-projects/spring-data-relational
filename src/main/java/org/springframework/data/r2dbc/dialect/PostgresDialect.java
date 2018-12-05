package org.springframework.data.r2dbc.dialect;

import lombok.RequiredArgsConstructor;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * An SQL dialect for Postgres.
 *
 * @author Mark Paluch
 */
public class PostgresDialect implements Dialect {

	private static final Set<Class<?>> SIMPLE_TYPES = new HashSet<>(
			Arrays.asList(UUID.class, URL.class, URI.class, InetAddress.class));

	/**
	 * Singleton instance.
	 */
	public static final PostgresDialect INSTANCE = new PostgresDialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getClause(long limit, long offset) {
			return String.format("LIMIT %d OFFSET %d", limit, offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long)
		 */
		@Override
		public String getClause(long limit) {
			return "LIMIT " + limit;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.END;
		}
	};

	private final PostgresArrayColumns ARRAY_COLUMNS = new PostgresArrayColumns(getSimpleTypeHolder());

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
	 * @see org.springframework.data.r2dbc.dialect.Dialect#returnGeneratedKeys()
	 */
	@Override
	public String generatedKeysClause() {
		return "RETURNING *";
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
	 * @see org.springframework.data.r2dbc.dialect.Dialect#limit()
	 */
	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	@RequiredArgsConstructor
	static class PostgresArrayColumns implements ArrayColumns {

		private final SimpleTypeHolder simpleTypes;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.ArrayColumns#isSupported()
		 */
		@Override
		public boolean isSupported() {
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.ArrayColumns#getArrayType(java.lang.Class)
		 */
		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Assert.notNull(userType, "Array component type must not be null");

			if (!simpleTypes.isSimpleType(userType)) {
				throw new IllegalArgumentException("Unsupported array type: " + ClassUtils.getQualifiedName(userType));
			}

			return ClassUtils.resolvePrimitiveIfNecessary(userType);
		}
	}
}
