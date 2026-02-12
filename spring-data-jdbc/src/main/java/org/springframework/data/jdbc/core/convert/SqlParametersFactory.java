/*
 * Copyright 2022-present the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.RelationalPredicates;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Creates the {@link SqlIdentifierParameterSource} for various SQL operations, dialect identifier processing rules and
 * applicable converters.
 *
 * @author Jens Schauder
 * @author Chirag Tailor
 * @author Mikhail Polivakha
 * @author Mark Paluch
 * @since 2.4
 */
public class SqlParametersFactory {

	private final RelationalMappingContext context;
	private final JdbcConverter converter;

	/**
	 * @since 4.0
	 */
	public SqlParametersFactory(JdbcConverter converter) {
		this(converter.getMappingContext(), converter);
	}

	/**
	 * @since 3.1
	 */
	public SqlParametersFactory(RelationalMappingContext context, JdbcConverter converter) {
		this.context = context;
		this.converter = converter;
	}

	/**
	 * Creates the parameters for a SQL insert operation.
	 *
	 * @param instance the entity to be inserted. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @param identifier information about data that needs to be considered for the insert but which is not part of the
	 *          entity. Namely references back to a parent entity and key/index columns for entities that are stored in a
	 *          {@link Map} or {@link List}.
	 * @param idValueSource the {@link IdValueSource} for the insert.
	 * @return the {@link SqlIdentifierParameterSource} for the insert. Guaranteed to not be {@code null}.
	 * @since 2.4
	 */
	<T> SqlIdentifierParameterSource forInsert(T instance, Class<T> domainType, Identifier identifier,
			IdValueSource idValueSource) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);
		ParameterSourceHolder holder = getParameterSource(instance, persistentEntity, "",
				PersistentProperty::isIdProperty);

		identifier.forEach(holder::addValue);

		if (IdValueSource.PROVIDED.equals(idValueSource)) {

			PersistentPropertyPathAccessor<T> propertyPathAccessor = persistentEntity.getPropertyPathAccessor(instance);
			AggregatePath.ColumnInfos columnInfos = context.getAggregatePath(persistentEntity).getTableInfo().idColumnInfos();

			// fullPath: because we use the result with a PropertyPathAccessor
			columnInfos.forEach((ap, __) -> {
				Object idValue = propertyPathAccessor.getProperty(ap.getRequiredPersistentPropertyPath());
				RelationalPersistentProperty idProperty = ap.getRequiredLeafProperty();
				holder.addValue(idProperty, idValue);
			});

		}
		return holder.getParameterSource();
	}

	/**
	 * Creates the parameters for a SQL update operation.
	 *
	 * @param instance the entity to be updated. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @return the {@link SqlIdentifierParameterSource} for the update. Guaranteed to not be {@code null}.
	 * @since 2.4
	 */
	<T> SqlIdentifierParameterSource forUpdate(T instance, Class<T> domainType) {
		return getParameterSource(instance, getRequiredPersistentEntity(domainType), "",
				RelationalPersistentProperty::isInsertOnly).getParameterSource();
	}

	/**
	 * Creates the parameters for a SQL query by id.
	 *
	 * @param id the entity id. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @return the {@link SqlIdentifierParameterSource} for the query. Guaranteed to not be {@code null}.
	 * @since 2.4
	 */
	<T> SqlIdentifierParameterSource forQueryById(Object id, Class<T> domainType) {

		return doWithIdentifiers(domainType, (columns, idProperty, complexId) -> {

			ParameterSourceHolder holder = new ParameterSourceHolder();
			BiFunction<Object, AggregatePath, Object> valueExtractor = getIdMapper(complexId);

			columns.forEach((ap, ci) -> holder.addValue(ci.name(), //
					ap.getRequiredLeafProperty(), valueExtractor.apply(id, ap)));

			return holder.getParameterSource();
		});
	}

	/**
	 * Creates the parameters for a SQL query by ids.
	 *
	 * @param ids the entity ids. Must not be {@code null}.
	 * @param domainType the type of the instance. Must not be {@code null}.
	 * @return the {@link SqlIdentifierParameterSource} for the query. Guaranteed to not be {@code null}.
	 * @since 2.4
	 */
	<T> SqlIdentifierParameterSource forQueryByIds(Iterable<?> ids, Class<T> domainType) {

		return doWithIdentifiers(domainType, (columns, idProperty, complexId) -> {

			ParameterSourceHolder holder = new ParameterSourceHolder();
			BiFunction<Object, AggregatePath, Object> valueExtractor = getIdMapper(complexId);

			List<@Nullable Object> parameterValues = new ArrayList<>(ids instanceof Collection<?> c ? c.size() : 16);

			if (complexId == null || columns.size() == 1) {

				for (Object id : ids) {
					appendIdentifier(holder, columns, id, valueExtractor, parameterValues);
				}
			} else {
				for (Object id : ids) {

					List<@Nullable Object> tuple = new ArrayList<>(columns.size());
					appendIdentifier(holder, columns, id, valueExtractor, tuple);
					parameterValues.add(tuple.toArray(new Object[0]));
				}
			}

			holder.getParameterSource().addValue(SqlGenerator.IDS_SQL_PARAMETER, parameterValues);
			return holder.getParameterSource();
		});
	}

	private static void appendIdentifier(ParameterSourceHolder holder, AggregatePath.ColumnInfos columns, Object id,
			BiFunction<Object, AggregatePath, Object> valueExtractor, List<@Nullable Object> tuple) {

		columns.forEach((ap, ci) -> {
			JdbcValue writeValue = holder.getWriteValue(ap.getRequiredLeafProperty(), valueExtractor.apply(id, ap));
			tuple.add(writeValue.getValue());
		});
	}

	private <T> T doWithIdentifiers(Class<?> domainType, IdentifierCallback<T> callback) {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(domainType);
		RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
		RelationalPersistentEntity<?> complexId = context.getPersistentEntity(idProperty);
		AggregatePath.ColumnInfos columns = context.getAggregatePath(entity).getTableInfo().idColumnInfos();

		return callback.doWithIdentifiers(columns, idProperty, complexId);
	}

	interface IdentifierCallback<T> {

		T doWithIdentifiers(AggregatePath.ColumnInfos columns, RelationalPersistentProperty idProperty,
				@Nullable RelationalPersistentEntity<?> complexId);
	}

	/**
	 * Creates the parameters for a SQL query of related entities.
	 *
	 * @param identifier the identifier describing the relation. Must not be {@code null}.
	 * @return the {@link SqlIdentifierParameterSource} for the query. Guaranteed to not be {@code null}.
	 * @since 2.4
	 */
	SqlIdentifierParameterSource forQueryByIdentifier(Identifier identifier) {

		ParameterSourceHolder holder = new ParameterSourceHolder();
		identifier.forEach(holder::addValue);
		return holder.getParameterSource();
	}

	private BiFunction<Object, AggregatePath, @Nullable Object> getIdMapper(
			@Nullable RelationalPersistentEntity<?> complexId) {

		if (complexId == null) {
			return (id, aggregatePath) -> id;
		}

		return (id, aggregatePath) -> {

			PersistentPropertyAccessor<Object> accessor = complexId.getPropertyAccessor(id);
			return accessor.getProperty(aggregatePath.getRequiredLeafProperty());
		};
	}

	@SuppressWarnings("unchecked")
	private <S> RelationalPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (RelationalPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	private <S, T> ParameterSourceHolder getParameterSource(@Nullable S instance,
			RelationalPersistentEntity<S> persistentEntity, String prefix,
			Predicate<RelationalPersistentProperty> skipProperty) {

		ParameterSourceHolder holder = new ParameterSourceHolder();

		PersistentPropertyAccessor<S> propertyAccessor = instance != null ? persistentEntity.getPropertyAccessor(instance)
				: NoValuePropertyAccessor.instance();

		persistentEntity.doWithAll(property -> {

			if (skipProperty.test(property) || !property.isWritable()) {
				return;
			}

			if (RelationalPredicates.isRelation(property)) {
				return;
			}

			if (property.isEmbedded()) {

				Object value = propertyAccessor.getProperty(property);
				RelationalPersistentEntity<?> embeddedEntity = context
						.getRequiredPersistentEntity(property.getTypeInformation());
				ParameterSourceHolder additionalParameters = getParameterSource((T) value,
						(RelationalPersistentEntity<T>) embeddedEntity, prefix + property.getEmbeddedPrefix(), skipProperty);
				holder.addAll(additionalParameters);
			} else {

				SqlIdentifier paramName = property.getColumnName().transform(prefix::concat);
				holder.addValue(paramName, property, propertyAccessor.getProperty(property));
			}
		});

		return holder;
	}

	/**
	 * A {@link PersistentPropertyAccessor} implementation always returning null
	 *
	 * @param <T>
	 */
	static class NoValuePropertyAccessor<T> implements PersistentPropertyAccessor<T> {

		private static final NoValuePropertyAccessor<?> INSTANCE = new NoValuePropertyAccessor<>();

		@SuppressWarnings("unchecked")
		static <T> NoValuePropertyAccessor<T> instance() {
			return (NoValuePropertyAccessor<T>) INSTANCE;
		}

		@Override
		public void setProperty(PersistentProperty<?> property, @Nullable Object value) {
			throw new UnsupportedOperationException("Cannot set value on 'null' target object");
		}

		@Override
		public @Nullable Object getProperty(PersistentProperty<?> property) {
			return null;
		}

		@Override
		public T getBean() {
			throw new UnsupportedOperationException("Cannot get bean of NoValuePropertyAccessor");
		}
	}

	/**
	 * Holder for a {@link SqlIdentifierParameterSource} that allows to add values with conversion.
	 *
	 * @since 4.0.3
	 */
	class ParameterSourceHolder {

		private final SqlIdentifierParameterSource parameterSource = new SqlIdentifierParameterSource();

		/**
		 * Add a value for the given property. The value will be converted using the configured converters and the column
		 * type. Uses {@link RelationalPersistentProperty#getColumnName()} as parameter name.
		 *
		 * @param property the property for which the value is added. Must not be {@code null}.
		 * @param value the value to add. Can be {@code null}.
		 */
		public void addValue(RelationalPersistentProperty property, @Nullable Object value) {
			addValue(property.getColumnName(), property, value);
		}

		/**
		 * Add a value for the given property. The value will be converted using the configured converters and the column
		 * type.
		 *
		 * @param paramName binding parameter name.
		 * @param property the property for which the value is added. Must not be {@code null}.
		 * @param value the value to add. Can be {@code null}.
		 */
		public void addValue(SqlIdentifier paramName, RelationalPersistentProperty property, @Nullable Object value) {

			JdbcValue jdbcValue = getWriteValue(property, value);
			parameterSource.addValue(paramName, jdbcValue);
		}

		/**
		 * Add a value for the given property. The value will be converted using the configured converters and the column.
		 *
		 * @param paramName binding parameter name.
		 * @param value the value to add. Can be {@code null}.
		 * @param javaType java type to determine the SQL target type.
		 */
		public void addValue(SqlIdentifier paramName, @Nullable Object value, Class<?> javaType) {

			JdbcValue jdbcValue = converter.writeJdbcValue(value, javaType, JdbcUtil.targetSqlTypeFor(javaType));
			parameterSource.addValue(paramName, jdbcValue);
		}

		JdbcValue getWriteValue(RelationalPersistentProperty property, @Nullable Object value) {
			return converter.writeJdbcValue(value, converter.getColumnType(property), converter.getTargetSqlType(property));
		}

		/**
		 * Add all parameters from the given {@link ParameterSourceHolder}.
		 *
		 * @param others the other parameter source holder. Must not be {@code null}.
		 */
		public void addAll(ParameterSourceHolder others) {
			this.parameterSource.addAll(others.parameterSource);
		}

		public SqlIdentifierParameterSource getParameterSource() {
			return parameterSource;
		}

	}
}
