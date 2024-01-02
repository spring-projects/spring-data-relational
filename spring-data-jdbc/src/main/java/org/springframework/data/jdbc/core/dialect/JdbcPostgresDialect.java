/*
 * Copyright 2021-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.dialect;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.postgresql.core.Oid;
import org.postgresql.jdbc.TypeInfoCache;
import org.springframework.data.jdbc.core.convert.JdbcArrayColumns;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.util.ClassUtils;

/**
 * JDBC specific Postgres Dialect.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.3
 */
public class JdbcPostgresDialect extends PostgresDialect implements JdbcDialect {

	public static final JdbcPostgresDialect INSTANCE = new JdbcPostgresDialect();

	private static final JdbcPostgresArrayColumns ARRAY_COLUMNS = new JdbcPostgresArrayColumns();

	private static final Set<Class<?>> SIMPLE_TYPES;

	static {

		Set<Class<?>> simpleTypes = new HashSet<>(PostgresDialect.INSTANCE.simpleTypes());
		List<String> simpleTypeNames = Arrays.asList( //
				"org.postgresql.util.PGobject", //
				"org.postgresql.geometric.PGpoint", //
				"org.postgresql.geometric.PGbox", //
				"org.postgresql.geometric.PGcircle", //
				"org.postgresql.geometric.PGline", //
				"org.postgresql.geometric.PGpath", //
				"org.postgresql.geometric.PGpolygon", //
				"org.postgresql.geometric.PGlseg" //
		);
		simpleTypeNames.forEach(name -> ifClassPresent(name, simpleTypes::add));
		SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	@Override
	public Set<Class<?>> simpleTypes() {
		return SIMPLE_TYPES;
	}

	@Override
	public JdbcArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	/**
	 * If the class is present on the class path, invoke the specified consumer {@code action} with the class object,
	 * otherwise do nothing.
	 *
	 * @param action block to be executed if a value is present.
	 */
	private static void ifClassPresent(String className, Consumer<Class<?>> action) {
		if (ClassUtils.isPresent(className, PostgresDialect.class.getClassLoader())) {
			action.accept(ClassUtils.resolveClassName(className, PostgresDialect.class.getClassLoader()));
		}
	}

	static class JdbcPostgresArrayColumns implements JdbcArrayColumns {

		private static final boolean TYPE_INFO_PRESENT = ClassUtils.isPresent("org.postgresql.jdbc.TypeInfoCache",
				JdbcPostgresDialect.class.getClassLoader());

		private static final TypeInfoWrapper TYPE_INFO_WRAPPER;

		static {
			TYPE_INFO_WRAPPER = TYPE_INFO_PRESENT ? new TypeInfoCacheWrapper() : new TypeInfoWrapper();
		}

		@Override
		public boolean isSupported() {
			return true;
		}

		@Override
		public SQLType getSqlType(Class<?> componentType) {

			SQLType sqlType = TYPE_INFO_WRAPPER.getArrayTypeMap().get(componentType);
			if (sqlType != null) {
				return sqlType;
			}

			return JdbcArrayColumns.super.getSqlType(componentType);
		}

		@Override
		public String getArrayTypeName(SQLType jdbcType) {

			if (jdbcType == JDBCType.DOUBLE) {
				return "FLOAT8";
			}
			if (jdbcType == JDBCType.REAL) {
				return "FLOAT4";
			}

			return jdbcType.getName();
		}
	}

	/**
	 * Wrapper for Postgres types. Defaults to no-op to guard runtimes against absent TypeInfoCache.
	 *
	 * @since 3.1.3
	 */
	static class TypeInfoWrapper {

		/**
		 * @return a type map between a Java array component type and its Postgres type.
		 */
		Map<Class<?>, SQLType> getArrayTypeMap() {
			return Collections.emptyMap();
		}
	}

	/**
	 * {@link TypeInfoWrapper} backed by {@link TypeInfoCache}.
	 *
	 * @since 3.1.3
	 */
	static class TypeInfoCacheWrapper extends TypeInfoWrapper {

		private final Map<Class<?>, SQLType> arrayTypes = new HashMap<>();

		public TypeInfoCacheWrapper() {

			TypeInfoCache cache = new TypeInfoCache(null, 0);
			addWellKnownTypes(cache);

			Iterator<String> it = cache.getPGTypeNamesWithSQLTypes();

			try {

				while (it.hasNext()) {

					String pgTypeName = it.next();
					int oid = cache.getPGType(pgTypeName);
					String javaClassName = cache.getJavaClass(oid);
					int arrayOid = cache.getJavaArrayType(pgTypeName);

					if (!ClassUtils.isPresent(javaClassName, getClass().getClassLoader())) {
						continue;
					}

					Class<?> javaClass = ClassUtils.forName(javaClassName, getClass().getClassLoader());

					// avoid accidental usage of smaller database types that map to the same Java type or generic-typed SQL
					// arrays.
					if (javaClass == Array.class || javaClass == String.class || javaClass == Integer.class || oid == Oid.OID
							|| oid == Oid.MONEY) {
						continue;
					}

					arrayTypes.put(javaClass, new PGSQLType(pgTypeName, arrayOid));
				}
			} catch (SQLException | ClassNotFoundException e) {
				throw new IllegalStateException("Cannot create type info mapping", e);
			}
		}

		private static void addWellKnownTypes(TypeInfoCache cache) {
			cache.addCoreType("uuid", Oid.UUID, Types.OTHER, UUID.class.getName(), Oid.UUID_ARRAY);
		}

		@Override
		Map<Class<?>, SQLType> getArrayTypeMap() {
			return arrayTypes;
		}

		record PGSQLType(String name, int oid) implements SQLType {

			@Override
			public String getName() {
				return name;
			}

			@Override
			public String getVendor() {
				return "Postgres";
			}

			@Override
			public Integer getVendorTypeNumber() {
				return oid;
			}
		}
	}
}
