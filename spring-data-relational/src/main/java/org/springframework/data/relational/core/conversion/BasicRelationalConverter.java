/*
 * Copyright 2018-2022 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.core.ResolvableType;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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
public class BasicRelationalConverter implements RelationalConverter {

	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context;
	private final ConfigurableConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final CustomConversions conversions;

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public BasicRelationalConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
		this(context, new CustomConversions(StoreConversions.NONE, Collections.emptyList()), new DefaultConversionService(),
				new EntityInstantiators());
	}

	/**
	 * Creates a new {@link BasicRelationalConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 */
	public BasicRelationalConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions) {
		this(context, conversions, new DefaultConversionService(), new EntityInstantiators());
	}

	@SuppressWarnings("unchecked")
	private BasicRelationalConverter(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			CustomConversions conversions, ConfigurableConversionService conversionService,
			EntityInstantiators entityInstantiators) {

		Assert.notNull(context, "MappingContext must not be null!");
		Assert.notNull(conversions, "CustomConversions must not be null!");

		this.context = (MappingContext) context;
		this.conversionService = conversionService;
		this.entityInstantiators = entityInstantiators;
		this.conversions = conversions;

		conversions.registerConvertersIn(this.conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#getConversionService()
	 */
	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}

	public CustomConversions getConversions() {
		return conversions;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#getMappingContext()
	 */
	@Override
	public MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return context;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#getPropertyAccessor(org.springframework.data.mapping.PersistentEntity, java.lang.Object)
	 */
	@Override
	public <T> PersistentPropertyAccessor<T> getPropertyAccessor(PersistentEntity<T, ?> persistentEntity, T instance) {

		PersistentPropertyAccessor<T> accessor = persistentEntity.getPropertyAccessor(instance);
		return new ConvertingPropertyAccessor<>(accessor, conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.RelationalConverter#createInstance(org.springframework.data.mapping.PersistentEntity, java.util.function.Function)
	 */
	@Override
	public <T> T createInstance(PersistentEntity<T, RelationalPersistentProperty> entity,
			Function<Parameter<?, RelationalPersistentProperty>, Object> parameterValueProvider) {

		return entityInstantiators.getInstantiatorFor(entity) //
				.createInstance(entity, new ConvertingParameterValueProvider<>(parameterValueProvider));
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

			TypeDescriptor sourceDescriptor = TypeDescriptor.valueOf(value.getClass());
			TypeDescriptor targetDescriptor = createTypeDescriptor(type);

			return getConversionService().convert(value, sourceDescriptor, targetDescriptor);
		}

		return getPotentiallyConvertedSimpleRead(value, type.getType());
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

		if (getConversions().isSimpleType(value.getClass())) {

			if (ClassTypeInformation.OBJECT != type) {

				if (conversionService.canConvert(value.getClass(), type.getType())) {
					value = conversionService.convert(value, type.getType());
				}
			}

			return getPotentiallyConvertedSimpleWrite(value);
		}

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(value.getClass());

		if (persistentEntity != null) {

			Object id = persistentEntity.getIdentifierAccessor(value).getIdentifier();
			return writeValue(id, type);
		}

		return conversionService.convert(value, type.getType());
	}

	@Override
	public EntityInstantiators getEntityInstantiators() {
		return this.entityInstantiators;
	}

	/**
	 * Checks whether we have a custom conversion registered for the given value into an arbitrary simple JDBC type.
	 * Returns the converted value if so. If not, we perform special enum handling or simply return the value as is.
	 *
	 * @param value to be converted. Must not be {@code null}.
	 * @return the converted value if a conversion applies or the original value. Might return {@code null}.
	 */
	@Nullable
	private Object getPotentiallyConvertedSimpleWrite(Object value) {

		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(value.getClass());

		if (customTarget.isPresent()) {
			return conversionService.convert(value, customTarget.get());
		}

		return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value to be converted. May be {@code null}..
	 * @param target may be {@code null}..
	 * @return the converted value if a conversion applies or the original value. Might return {@code null}.
	 */
	@Nullable
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Object getPotentiallyConvertedSimpleRead(@Nullable Object value, @Nullable Class<?> target) {

		if (value == null || target == null || ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (Enum.class.isAssignableFrom(target)) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return conversionService.convert(value, target);
	}

	protected static TypeDescriptor createTypeDescriptor(TypeInformation<?> type) {

		List<TypeInformation<?>> typeArguments = type.getTypeArguments();
		Class<?>[] generics = new Class[typeArguments.size()];
		for (int i = 0; i < typeArguments.size(); i++) {
			generics[i] = typeArguments.get(i).getType();
		}

		return new TypeDescriptor(ResolvableType.forClassWithGenerics(type.getType(), generics), type.getType(), null);
	}

	/**
	 * Converter-aware {@link ParameterValueProvider}.
	 *
	 * @param <P>
	 * @author Mark Paluch
	 */
	class ConvertingParameterValueProvider<P extends PersistentProperty<P>> implements ParameterValueProvider<P> {

		private final Function<Parameter<?, P>, Object> delegate;

		ConvertingParameterValueProvider(Function<Parameter<?, P>, Object> delegate) {

			Assert.notNull(delegate, "Delegate must not be null.");

			this.delegate = delegate;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.model.ParameterValueProvider#getParameterValue(org.springframework.data.mapping.PreferredConstructor.Parameter)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public <T> T getParameterValue(Parameter<T, P> parameter) {
			return (T) readValue(delegate.apply(parameter), parameter.getType());
		}
	}
}
