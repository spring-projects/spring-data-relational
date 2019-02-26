/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.convert;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * {@link RelationalConverter} that uses a {@link MappingContext} to apply basic conversion of relational values to
 * property values.
 * <p>
 * Conversion is configurable by providing a customized {@link CustomConversions}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @see MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 */
public class BasicJdbcConverter extends BasicRelationalConverter implements JdbcConverter {

	private static final Logger LOG = LoggerFactory.getLogger(BasicJdbcConverter.class);

	private final JdbcTypeFactory typeFactory;

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}. org.springframework.data.jdbc.core.DefaultDataAccessStrategyUnitTests
	 * @param typeFactory
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			JdbcTypeFactory typeFactory) {
		super(context);
		this.typeFactory = typeFactory;
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and {@link CustomConversions}.
	 * 
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 * @param typeFactory
	 */
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions, JdbcTypeFactory typeFactory) {
		super(context, conversions);
		this.typeFactory = typeFactory;
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}. org.springframework.data.jdbc.core.DefaultDataAccessStrategyUnitTests
	 * @deprecated use one of the constructors with {@link JdbcTypeFactory} parameter.
	 */
	@Deprecated
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		this(context, JdbcTypeFactory.DUMMY_JDBC_TYPE_FACTORY);
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 * @deprecated use one of the constructors with {@link JdbcTypeFactory} parameter.
	 */
	@Deprecated
	public BasicJdbcConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions) {
		this(context, conversions, JdbcTypeFactory.DUMMY_JDBC_TYPE_FACTORY);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#readValue(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	@Nullable
	public Object readValue(@Nullable Object value, TypeInformation<?> type) {

		if (null == value) {
			return null;
		}

		if (getConversions().hasCustomReadTarget(value.getClass(), type.getType())) {
			return getConversionService().convert(value, type.getType());
		}

		if (AggregateReference.class.isAssignableFrom(type.getType())) {

			TypeInformation<?> idType = type.getSuperTypeInformation(AggregateReference.class).getTypeArguments().get(1);

			return AggregateReference.to(readValue(value, idType));
		}

		if (value instanceof Array) {
			try {
				return readValue(((Array) value).getArray(), type);
			} catch (SQLException | ConverterNotFoundException e) {
				LOG.info("Failed to extract a value of type %s from an Array. Attempting to use standard conversions.", e);
			}
		}

		return super.readValue(value, type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#writeValue(java.lang.Object, org.springframework.data.util.TypeInformation)
	 */
	@Override
	@Nullable
	public Object writeValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return null;
		}

		if (AggregateReference.class.isAssignableFrom(value.getClass())) {
			return writeValue(((AggregateReference) value).getId(), type);
		}

		return super.writeValue(value, type);
	}

	@Override
	public boolean canWriteValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return true;
		}

		if (AggregateReference.class.isAssignableFrom(value.getClass())) {
			return canWriteValue(((AggregateReference) value).getId(), type);
		}
		return super.canWriteValue(value, type);
	}

	public JdbcTypeAware writeTypeAware(@Nullable Object value, Class<?> columnType, int sqlType) {

		JdbcTypeAware jdbcTypeAware = tryToConvertToJdbcTypeAware(value);
		if (jdbcTypeAware != null) {
			return jdbcTypeAware;
		}

		Object convertedValue = writeValue(value, ClassTypeInformation.from(columnType));

		if (convertedValue == null || !convertedValue.getClass().isArray()) {
			return JdbcTypeAware.of(convertedValue, JdbcUtil.jdbcTypeFor(sqlType));
		}

		Class<?> componentType = convertedValue.getClass().getComponentType();
		if (componentType != byte.class && componentType != Byte.class) {
			return JdbcTypeAware.of(typeFactory.createArray((Object[]) convertedValue), JDBCType.ARRAY);
		}

		if (componentType == Byte.class) {
			Byte[] boxedBytes = (Byte[]) convertedValue;
			byte[] bytes = new byte[boxedBytes.length];
			for (int i = 0; i < boxedBytes.length; i++) {
				bytes[i] = boxedBytes[i];
			}
			convertedValue = bytes;
		}

		return JdbcTypeAware.of(convertedValue, JDBCType.BINARY);

	}

	private JdbcTypeAware tryToConvertToJdbcTypeAware(@Nullable Object value) {

		JdbcTypeAware jdbcTypeAware = null;
		ClassTypeInformation<JdbcTypeAware> jdbcTypeAwareClassTypeInformation = ClassTypeInformation
				.from(JdbcTypeAware.class);
		if (canWriteValue(value, jdbcTypeAwareClassTypeInformation)) {

			try {

				jdbcTypeAware = (JdbcTypeAware) writeValue(value, jdbcTypeAwareClassTypeInformation);

			} catch (ConversionException __) {
				// a conversion may still fail
			}
		}
		return jdbcTypeAware;
	}
}
