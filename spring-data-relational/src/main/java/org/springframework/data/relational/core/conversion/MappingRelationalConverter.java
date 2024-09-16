/*
 * Copyright 2023-2024 the original author or authors.
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.env.Environment;
import org.springframework.core.env.EnvironmentCapable;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.InstanceCreatorMetadata;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.*;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.EntityProjectionIntrospector.ProjectionPredicate;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * {@link org.springframework.data.relational.core.conversion.RelationalConverter} that uses a
 * {@link org.springframework.data.mapping.context.MappingContext} to apply sophisticated mapping of domain objects from
 * {@link org.springframework.data.relational.domain.RowDocument}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Chirag Tailor
 * @author Vincent Galloy
 * @author Chanhyeong Cho
 * @author Lukáš Křečan
 * @see org.springframework.data.mapping.context.MappingContext
 * @see SimpleTypeHolder
 * @see CustomConversions
 * @since 3.2
 */
public class MappingRelationalConverter extends AbstractRelationalConverter
		implements ApplicationContextAware, EnvironmentCapable {

	private SpELContext spELContext;

	private @Nullable Environment environment;

	private final ExpressionParser expressionParser = new SpelExpressionParser();

	private final SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory(
			expressionParser);

	private final EntityProjectionIntrospector introspector;

	private final CachingValueExpressionEvaluatorFactory valueExpressionEvaluatorFactory = new CachingValueExpressionEvaluatorFactory(
			expressionParser, this, o -> spELContext.getEvaluationContext(o));

	/**
	 * Creates a new {@link MappingRelationalConverter} given the new {@link RelationalMappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingRelationalConverter(RelationalMappingContext context) {

		super(context);

		this.spELContext = new SpELContext(DocumentPropertyAccessor.INSTANCE);
		this.introspector = createIntrospector(projectionFactory, getConversions(), getMappingContext());
	}

	/**
	 * Creates a new {@link MappingRelationalConverter} given the new {@link RelationalMappingContext} and
	 * {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 */
	public MappingRelationalConverter(RelationalMappingContext context, CustomConversions conversions) {

		super(context, conversions);

		this.spELContext = new SpELContext(DocumentPropertyAccessor.INSTANCE);
		this.introspector = createIntrospector(projectionFactory, getConversions(), getMappingContext());
	}

	private static EntityProjectionIntrospector createIntrospector(ProjectionFactory projectionFactory,
			CustomConversions conversions, MappingContext<?, ?> mappingContext) {

		return EntityProjectionIntrospector.create(projectionFactory,
				ProjectionPredicate.typeHierarchy().and((target, underlyingType) -> !conversions.isSimpleType(target)),
				mappingContext);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		this.spELContext = new SpELContext(this.spELContext, applicationContext);
		this.environment = applicationContext.getEnvironment();
		this.projectionFactory.setBeanFactory(applicationContext);
		this.projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
	}

	@Override
	public Environment getEnvironment() {

		if (this.environment == null) {
			this.environment = new StandardEnvironment();
		}
		return this.environment;
	}

	/**
	 * Creates a new {@link ConversionContext}.
	 *
	 * @return the {@link ConversionContext}.
	 */
	protected ConversionContext getConversionContext(ObjectPath path) {

		Assert.notNull(path, "ObjectPath must not be null");

		return new DefaultConversionContext(this, getConversions(), path, this::readAggregate, this::readCollectionOrArray,
				this::readMap, this::getPotentiallyConvertedSimpleRead);
	}

	@Override
	public <T> PersistentPropertyPathAccessor<T> getPropertyAccessor(PersistentEntity<T, ?> persistentEntity,
			T instance) {

		PersistentPropertyPathAccessor<T> accessor = persistentEntity.getPropertyPathAccessor(instance);
		return new ConvertingPropertyAccessor<>(accessor, getConversionService());
	}

	@Override
	public <M, D> EntityProjection<M, D> introspectProjection(Class<M> resultType, Class<D> entityType) {

		RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(entityType);
		if (persistentEntity == null && !resultType.isInterface()
				|| ClassUtils.isAssignable(RowDocument.class, resultType)) {
			return (EntityProjection) EntityProjection.nonProjecting(resultType);
		}
		return introspector.introspect(resultType, entityType);
	}

	@Override
	public <R> R project(EntityProjection<R, ?> projection, RowDocument document) {

		if (!projection.isProjection()) { // backed by real object

			TypeInformation<?> typeToRead = projection.getMappedType().getType().isInterface() ? projection.getDomainType()
					: projection.getMappedType();
			return (R) read(typeToRead, document);
		}

		ProjectingConversionContext context = newProjectingConversionContext(projection);
		return doReadProjection(context, document, projection);
	}

	protected <R> ProjectingConversionContext newProjectingConversionContext(EntityProjection<R, ?> projection) {
		return new ProjectingConversionContext(this, getConversions(), ObjectPath.ROOT, this::readCollectionOrArray,
				this::readMap, this::getPotentiallyConvertedSimpleRead, projection);
	}

	@SuppressWarnings("unchecked")
	protected <R> R doReadProjection(ConversionContext context, RowDocument document, EntityProjection<R, ?> projection) {

		RelationalPersistentEntity<?> entity = getMappingContext()
				.getRequiredPersistentEntity(projection.getActualDomainType());
		TypeInformation<?> mappedType = projection.getActualMappedType();
		RelationalPersistentEntity<R> mappedEntity = (RelationalPersistentEntity<R>) getMappingContext()
				.getPersistentEntity(mappedType);
		ValueExpressionEvaluator evaluator = valueExpressionEvaluatorFactory.create(document);

		boolean isInterfaceProjection = mappedType.getType().isInterface();
		if (isInterfaceProjection) {

			PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(mappedEntity);
			RowDocumentAccessor documentAccessor = new RowDocumentAccessor(document);
			PersistentPropertyAccessor<?> accessor = new MapPersistentPropertyAccessor();

			PersistentPropertyAccessor<?> convertingAccessor = PropertyTranslatingPropertyAccessor
					.create(new ConvertingPropertyAccessor<>(accessor, getConversionService()), propertyTranslator);
			RelationalPropertyValueProvider valueProvider = newValueProvider(documentAccessor, evaluator, context);

			readProperties(context, entity, convertingAccessor, documentAccessor, valueProvider, Predicates.isTrue());
			return (R) projectionFactory.createProjection(mappedType.getType(), accessor.getBean());
		}

		// DTO projection
		if (mappedEntity == null) {
			throw new MappingException(String.format("No mapping metadata found for %s", mappedType.getType().getName()));
		}

		// create target instance, merge metadata from underlying DTO type
		PersistentPropertyTranslator propertyTranslator = PersistentPropertyTranslator.create(entity,
				Predicates.negate(RelationalPersistentProperty::hasExplicitColumnName));
		RowDocumentAccessor documentAccessor = new RowDocumentAccessor(document) {

			@Override
			String getColumnName(RelationalPersistentProperty prop) {
				return propertyTranslator.translate(prop).getColumnName().getReference();
			}
		};

		InstanceCreatorMetadata<RelationalPersistentProperty> instanceCreatorMetadata = mappedEntity
				.getInstanceCreatorMetadata();
		ParameterValueProvider<RelationalPersistentProperty> provider = instanceCreatorMetadata != null
				&& instanceCreatorMetadata.hasParameters()
						? getParameterProvider(context, mappedEntity, documentAccessor, evaluator)
						: NoOpParameterValueProvider.INSTANCE;

		EntityInstantiator instantiator = getEntityInstantiators().getInstantiatorFor(mappedEntity);
		R instance = instantiator.createInstance(mappedEntity, provider);

		return populateProperties(context, mappedEntity, documentAccessor, evaluator, instance);
	}

	private Object doReadOrProject(ConversionContext context, RowDocument source, TypeInformation<?> typeHint,
			EntityProjection<?, ?> typeDescriptor) {

		if (typeDescriptor.isProjection()) {
			return doReadProjection(context, source, typeDescriptor);
		}

		return readAggregate(context, source, typeHint);
	}

	static class MapPersistentPropertyAccessor implements PersistentPropertyAccessor<Map<String, Object>> {

		Map<String, Object> map = new LinkedHashMap<>();

		@Override
		public void setProperty(PersistentProperty<?> persistentProperty, Object o) {
			map.put(persistentProperty.getName(), o);
		}

		@Override
		public Object getProperty(PersistentProperty<?> persistentProperty) {
			return map.get(persistentProperty.getName());
		}

		@Override
		public Map<String, Object> getBean() {
			return map;
		}
	}

	/**
	 * Read a {@link RowDocument} into the requested {@link Class aggregate type}.
	 *
	 * @param type target aggregate type.
	 * @param source source {@link RowDocument}.
	 * @return the converted object.
	 * @param <R> aggregate type.
	 */
	@Override
	public <R> R read(Class<R> type, RowDocument source) {
		return read(TypeInformation.of(type), source);
	}

	protected <S> S read(TypeInformation<S> type, RowDocument source) {
		return readAggregate(getConversionContext(ObjectPath.ROOT), source, type);
	}

	/**
	 * Conversion method to materialize an object from a {@link RowDocument document}. Can be overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param document must not be {@literal null}
	 * @param typeHint the {@link TypeInformation} to be used to unmarshall this {@link RowDocument}.
	 * @return the converted object, will never be {@literal null}.
	 */
	protected <S> S readAggregate(ConversionContext context, RowDocument document,
			TypeInformation<? extends S> typeHint) {
		return readAggregate(context, new RowDocumentAccessor(document), typeHint);
	}

	/**
	 * Conversion method to materialize an object from a {@link RowDocument document}. Can be overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param documentAccessor must not be {@literal null}
	 * @param typeHint the {@link TypeInformation} to be used to unmarshall this {@link RowDocument}.
	 * @return the converted object, will never be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	protected <S> S readAggregate(ConversionContext context, RowDocumentAccessor documentAccessor,
			TypeInformation<? extends S> typeHint) {

		Class<? extends S> rawType = typeHint.getType();

		if (getConversions().hasCustomReadTarget(RowDocument.class, rawType)) {
			return doConvert(documentAccessor.getDocument(), rawType, typeHint.getType());
		}

		if (RowDocument.class.isAssignableFrom(rawType)) {
			return (S) documentAccessor.getDocument();
		}

		if (typeHint.isMap()) {
			return context.convert(documentAccessor, typeHint);
		}

		RelationalPersistentEntity<?> entity = getMappingContext().getPersistentEntity(typeHint);

		if (entity == null) {
			throw new MappingException(
					String.format("Expected to read Document %s into type %s but didn't find a PersistentEntity for the latter",
							documentAccessor, rawType));
		}

		return read(context, (RelationalPersistentEntity<S>) entity, documentAccessor);
	}

	/**
	 * Reads the given {@link RowDocument} into a {@link Map}. will recursively resolve nested {@link Map}s as well. Can
	 * be overridden by subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param source must not be {@literal null}
	 * @param targetType the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link RowDocument}.
	 * @return the converted {@link Map}, will never be {@literal null}.
	 */
	protected Map<Object, Object> readMap(ConversionContext context, Map<?, ?> source, TypeInformation<?> targetType) {

		Assert.notNull(source, "Document must not be null");
		Assert.notNull(targetType, "TypeInformation must not be null");

		Class<?> mapType = targetType.getType();

		TypeInformation<?> keyType = targetType.getComponentType();
		TypeInformation<?> valueType = targetType.getMapValueType() == null ? TypeInformation.OBJECT
				: targetType.getRequiredMapValueType();

		Class<?> rawKeyType = keyType != null ? keyType.getType() : Object.class;

		Map<Object, Object> map = CollectionFactory.createMap(mapType, rawKeyType,
				((Map<String, Object>) source).keySet().size());

		source.forEach((k, v) -> {

			Object key = k;

			if (!rawKeyType.isAssignableFrom(key.getClass())) {
				key = doConvert(key, rawKeyType);
			}

			map.put(key, v == null ? v : context.convert(v, valueType));
		});

		return map;
	}

	/**
	 * Reads the given {@link Collection} into a collection of the given {@link TypeInformation}. Can be overridden by
	 * subclasses.
	 *
	 * @param context must not be {@literal null}
	 * @param source must not be {@literal null}
	 * @param targetType the {@link Map} {@link TypeInformation} to be used to unmarshall this {@link RowDocument}.
	 * @return the converted {@link Collection} or array, will never be {@literal null}.
	 */
	protected Object readCollectionOrArray(ConversionContext context, Collection<?> source,
			TypeInformation<?> targetType) {

		Assert.notNull(targetType, "Target type must not be null");

		Class<?> collectionType = targetType.isSubTypeOf(Collection.class) //
				? targetType.getType() //
				: List.class;

		TypeInformation<?> componentType = targetType.getComponentType() != null //
				? targetType.getComponentType() //
				: TypeInformation.OBJECT;
		Class<?> rawComponentType = componentType.getType();

		Collection<Object> items = targetType.getType().isArray() //
				? new ArrayList<>(source.size()) //
				: CollectionFactory.createCollection(collectionType, rawComponentType, source.size());

		if (source.isEmpty()) {
			return getPotentiallyConvertedSimpleRead(items, targetType);
		}

		for (Object element : source) {
			items.add(element != null ? context.convert(element, componentType) : element);
		}

		return getPotentiallyConvertedSimpleRead(items, targetType);
	}

	private <T> T doConvert(Object value, Class<? extends T> target) {
		return doConvert(value, target, null);
	}

	@SuppressWarnings("ConstantConditions")
	private <T> T doConvert(Object value, Class<? extends T> target, @Nullable Class<? extends T> fallback) {

		if (getConversionService().canConvert(value.getClass(), target) || fallback == null) {
			return getConversionService().convert(value, target);
		}
		return getConversionService().convert(value, fallback);
	}

	private <S> S read(ConversionContext context, RelationalPersistentEntity<S> entity,
			RowDocumentAccessor documentAccessor) {

		ValueExpressionEvaluator evaluator = valueExpressionEvaluatorFactory.create(documentAccessor.getDocument());

		InstanceCreatorMetadata<RelationalPersistentProperty> instanceCreatorMetadata = entity.getInstanceCreatorMetadata();

		ParameterValueProvider<RelationalPersistentProperty> provider = instanceCreatorMetadata != null
				&& instanceCreatorMetadata.hasParameters() ? getParameterProvider(context, entity, documentAccessor, evaluator)
						: NoOpParameterValueProvider.INSTANCE;

		EntityInstantiator instantiator = getEntityInstantiators().getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		return populateProperties(context, entity, documentAccessor, evaluator, instance);
	}

	private ParameterValueProvider<RelationalPersistentProperty> getParameterProvider(ConversionContext context,
			RelationalPersistentEntity<?> entity, RowDocumentAccessor source, ValueExpressionEvaluator evaluator) {

		// Ensure that ConversionContext is contextualized to the current property.
		RelationalPropertyValueProvider contextualizing = new RelationalPropertyValueProvider() {
			@Override
			public boolean hasValue(RelationalPersistentProperty property) {
				return withContext(context.forProperty(property)).hasValue(property);
			}

			@Override
			public boolean hasNonEmptyValue(RelationalPersistentProperty property) {
				return withContext(context.forProperty(property)).hasNonEmptyValue(property);
			}

			@SuppressWarnings("unchecked")
			@Nullable
			@Override
			public <T> T getPropertyValue(RelationalPersistentProperty property) {

				ConversionContext propertyContext = context.forProperty(property);
				RelationalPropertyValueProvider provider = withContext(propertyContext);

				if (property.isEmbedded()) {
					return (T) readEmbedded(propertyContext, provider, source, property,
							getMappingContext().getRequiredPersistentEntity(property));
				}

				return provider.getPropertyValue(property);
			}

			@Override
			public RelationalPropertyValueProvider withContext(ConversionContext context) {
				return newValueProvider(source, evaluator, context);
			}
		};

		PersistentEntityParameterValueProvider<RelationalPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<>(
				entity, contextualizing, context.getPath().getCurrentObject());

		return new ConverterAwareExpressionParameterValueProvider(context, evaluator, getConversionService(),
				new ConvertingParameterValueProvider<>(parameterProvider::getParameterValue));
	}

	private <S> S populateProperties(ConversionContext context, RelationalPersistentEntity<S> entity,
			RowDocumentAccessor documentAccessor, ValueExpressionEvaluator evaluator, S instance) {

		if (!entity.requiresPropertyPopulation()) {
			return instance;
		}

		PersistentPropertyAccessor<S> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(instance),
				getConversionService());

		// Make sure id property is set before all other properties
		ObjectPath currentPath = context.getPath().push(accessor.getBean(), entity);
		ConversionContext contextToUse = context.withPath(currentPath);

		RelationalPropertyValueProvider valueProvider = newValueProvider(documentAccessor, evaluator, contextToUse);

		Predicate<RelationalPersistentProperty> propertyFilter = isConstructorArgument(entity).negate();
		readProperties(contextToUse, entity, accessor, documentAccessor, valueProvider, propertyFilter);

		return accessor.getBean();
	}

	protected RelationalPropertyValueProvider newValueProvider(RowDocumentAccessor documentAccessor,
			ValueExpressionEvaluator evaluator, ConversionContext context) {
		return new DocumentValueProvider(context, documentAccessor, evaluator, spELContext);
	}

	private void readProperties(ConversionContext context, RelationalPersistentEntity<?> entity,
			PersistentPropertyAccessor<?> accessor, RowDocumentAccessor documentAccessor,
			RelationalPropertyValueProvider valueProvider, Predicate<RelationalPersistentProperty> propertyFilter) {

		for (RelationalPersistentProperty property : entity) {

			if (!propertyFilter.test(property)) {
				continue;
			}

			ConversionContext propertyContext = context.forProperty(property);
			RelationalPropertyValueProvider valueProviderToUse = valueProvider.withContext(propertyContext);

			if (property.isEmbedded()) {
				accessor.setProperty(property, readEmbedded(propertyContext, valueProviderToUse, documentAccessor, property,
						getMappingContext().getRequiredPersistentEntity(property)));
				continue;
			}

			// this hasValue should actually check against null
			if (!valueProviderToUse.hasValue(property)) {
				continue;
			}

			accessor.setProperty(property, valueProviderToUse.getPropertyValue(property));
		}
	}

	@Nullable
	private Object readEmbedded(ConversionContext conversionContext, RelationalPropertyValueProvider provider,
			RowDocumentAccessor source, RelationalPersistentProperty property,
			RelationalPersistentEntity<?> persistentEntity) {

		if (shouldReadEmbeddable(conversionContext, property, persistentEntity, provider)) {
			return read(conversionContext, persistentEntity, source);
		}

		return null;
	}

	private boolean shouldReadEmbeddable(ConversionContext context, RelationalPersistentProperty property,
			RelationalPersistentEntity<?> unwrappedEntity, RelationalPropertyValueProvider propertyValueProvider) {

		OnEmpty onEmpty = property.getRequiredAnnotation(Embedded.class).onEmpty();

		if (onEmpty.equals(OnEmpty.USE_EMPTY)) {
			return true;
		}

		for (RelationalPersistentProperty persistentProperty : unwrappedEntity) {

			ConversionContext nestedContext = context.forProperty(persistentProperty);
			RelationalPropertyValueProvider contextual = propertyValueProvider.withContext(nestedContext);

			if (persistentProperty.isEmbedded()) {

				RelationalPersistentEntity<?> nestedEntity = getMappingContext()
						.getRequiredPersistentEntity(persistentProperty);

				if (shouldReadEmbeddable(nestedContext, persistentProperty, nestedEntity, contextual)) {
					return true;
				}

			} else if (contextual.hasNonEmptyValue(persistentProperty)) {
				return true;
			}
		}

		return false;
	}

	@Override
	@Nullable
	public Object readValue(@Nullable Object value, TypeInformation<?> type) {

		if (null == value) {
			return null;
		}

		return getPotentiallyConvertedSimpleRead(value, type);
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

		Optional<Class<?>> customTarget = getConversions().getCustomWriteTarget(value.getClass());

		if (customTarget.isPresent()) {
			return getConversionService().convert(value, customTarget.get());
		}

		return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
	}

	/**
	 * Checks whether we have a custom conversion for the given simple object. Converts the given value if so, applies
	 * {@link Enum} handling or returns the value as is.
	 *
	 * @param value to be converted. May be {@code null}..
	 * @param type {@link TypeInformation} into which the value is to be converted. Must not be {@code null}.
	 * @return the converted value if a conversion applies or the original value. Might return {@code null}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object getPotentiallyConvertedSimpleRead(Object value, TypeInformation<?> type) {

		Class<?> target = type.getType();

		if (getConversions().hasCustomReadTarget(value.getClass(), target)) {
			return getConversionService().convert(value, TypeDescriptor.forObject(value), createTypeDescriptor(type));
		}

		if (ClassUtils.isAssignableValue(target, value)) {
			return value;
		}

		if (Enum.class.isAssignableFrom(target) && value instanceof CharSequence) {
			return Enum.valueOf((Class<Enum>) target, value.toString());
		}

		return getConversionService().convert(value, TypeDescriptor.forObject(value), createTypeDescriptor(type));
	}

	private static TypeDescriptor createTypeDescriptor(TypeInformation<?> type) {

		List<TypeInformation<?>> typeArguments = type.getTypeArguments();
		Class<?>[] generics = new Class[typeArguments.size()];
		for (int i = 0; i < typeArguments.size(); i++) {
			generics[i] = typeArguments.get(i).getType();
		}

		return new TypeDescriptor(ResolvableType.forClassWithGenerics(type.getType(), generics), type.getType(), null);
	}

	@Override
	@Nullable
	public Object writeValue(@Nullable Object value, TypeInformation<?> type) {

		if (value == null) {
			return null;
		}

		if (getConversions().isSimpleType(value.getClass())) {

			Optional<Class<?>> customWriteTarget = getConversions().hasCustomWriteTarget(value.getClass(), type.getType())
					? getConversions().getCustomWriteTarget(value.getClass(), type.getType())
					: getConversions().getCustomWriteTarget(type.getType());

			if (customWriteTarget.isPresent()) {
				return getConversionService().convert(value, customWriteTarget.get());
			}

			if (TypeInformation.OBJECT != type) {

				if (type.getType().isAssignableFrom(value.getClass())) {

					if (value.getClass().isEnum()) {
						return getPotentiallyConvertedSimpleWrite(value);
					}

					return value;
				} else {
					if (getConversionService().canConvert(value.getClass(), type.getType())) {
						value = getConversionService().convert(value, type.getType());
					}
				}
			}

			return getPotentiallyConvertedSimpleWrite(value);
		}

		if (value.getClass().isArray()) {
			return writeArray(value, type);
		}

		if (value instanceof Collection<?>) {
			return writeCollection((Iterable<?>) value, type);
		}

		if (getMappingContext().hasPersistentEntityFor(value.getClass())) {

			RelationalPersistentEntity<?> persistentEntity = getMappingContext().getPersistentEntity(value.getClass());

			if (persistentEntity != null) {

				Object id = persistentEntity.getIdentifierAccessor(value).getIdentifier();
				return writeValue(id, type);
			}
		}

		return

		getConversionService().convert(value, type.getType());
	}

	private Object writeArray(Object value, TypeInformation<?> type) {

		Class<?> componentType = value.getClass().getComponentType();
		Optional<Class<?>> optionalWriteTarget = getConversions().getCustomWriteTarget(componentType);

		if (optionalWriteTarget.isEmpty() && !componentType.isEnum()) {
			return value;
		}

		Class<?> customWriteTarget = optionalWriteTarget
				.orElseGet(() -> componentType.isEnum() ? String.class : componentType);

		// optimization: bypass identity conversion
		if (customWriteTarget.equals(componentType)) {
			return value;
		}

		TypeInformation<?> component = TypeInformation.OBJECT;
		if (type.isCollectionLike() && type.getActualType() != null) {
			component = type.getRequiredComponentType();
		}

		int length = Array.getLength(value);
		Object target = Array.newInstance(customWriteTarget, length);
		for (int i = 0; i < length; i++) {
			Array.set(target, i, writeValue(Array.get(value, i), component));
		}

		return target;
	}

	private Object writeCollection(Iterable<?> value, TypeInformation<?> type) {

		List<Object> mapped = new ArrayList<>();

		TypeInformation<?> component = TypeInformation.OBJECT;
		if (type.isCollectionLike() && type.getActualType() != null) {
			component = type.getRequiredComponentType();
		}

		for (Object o : value) {
			mapped.add(writeValue(o, component));
		}

		if (type.getType().isInstance(mapped) || !type.isCollectionLike()) {
			return mapped;
		}

		return getConversionService().convert(mapped, type.getType());
	}

	static Predicate<RelationalPersistentProperty> isConstructorArgument(PersistentEntity<?, ?> entity) {
		return entity::isCreatorArgument;
	}

	/**
	 * Conversion context holding references to simple {@link ValueConverter} and {@link ContainerValueConverter}.
	 * Entrypoint for recursive conversion of {@link RowDocument} and other types.
	 *
	 * @since 3.2
	 */
	protected static class DefaultConversionContext implements ConversionContext {

		final RelationalConverter sourceConverter;
		final org.springframework.data.convert.CustomConversions conversions;
		final ObjectPath objectPath;
		final ContainerValueConverter<RowDocument> documentConverter;
		final ContainerValueConverter<Collection<?>> collectionConverter;
		final ContainerValueConverter<Map<?, ?>> mapConverter;
		final ValueConverter<Object> elementConverter;

		protected DefaultConversionContext(RelationalConverter sourceConverter,
				org.springframework.data.convert.CustomConversions customConversions, ObjectPath objectPath,
				ContainerValueConverter<RowDocument> documentConverter,
				ContainerValueConverter<Collection<?>> collectionConverter, ContainerValueConverter<Map<?, ?>> mapConverter,
				ValueConverter<Object> elementConverter) {

			this.sourceConverter = sourceConverter;
			this.conversions = customConversions;
			this.objectPath = objectPath;
			this.documentConverter = documentConverter;
			this.collectionConverter = collectionConverter;
			this.mapConverter = mapConverter;
			this.elementConverter = elementConverter;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <S> S convert(Object source, TypeInformation<? extends S> typeHint, ConversionContext context) {

			Assert.notNull(source, "Source must not be null");
			Assert.notNull(typeHint, "TypeInformation must not be null");

			if (conversions.hasCustomReadTarget(source.getClass(), typeHint.getType())) {
				return (S) elementConverter.convert(source, typeHint);
			}

			if (source instanceof Collection<?> collection) {

				if (typeHint.isCollectionLike() || typeHint.getType().isAssignableFrom(Collection.class)) {
					return (S) collectionConverter.convert(context, collection, typeHint);
				}
			}

			if (typeHint.isMap()) {

				if (ClassUtils.isAssignable(RowDocument.class, typeHint.getType())) {
					return (S) documentConverter.convert(context, (RowDocument) source, typeHint);
				}

				if (source instanceof Map<?, ?> map) {
					return (S) mapConverter.convert(context, map, typeHint);
				}

				throw new IllegalArgumentException(
						String.format("Expected map like structure but found %s", source.getClass()));
			}

			if (source instanceof RowDocument document) {
				return (S) documentConverter.convert(context, document, typeHint);
			}

			return (S) elementConverter.convert(source, typeHint);
		}

		@Override
		public ConversionContext withPath(ObjectPath currentPath) {

			Assert.notNull(currentPath, "ObjectPath must not be null");

			return new DefaultConversionContext(sourceConverter, conversions, currentPath, documentConverter,
					collectionConverter, mapConverter, elementConverter);
		}

		@Override
		public ObjectPath getPath() {
			return objectPath;
		}

		@Override
		public CustomConversions getCustomConversions() {
			return conversions;
		}

		@Override
		public RelationalConverter getSourceConverter() {
			return sourceConverter;
		}

		/**
		 * Converts a simple {@code source} value into {@link TypeInformation the target type}.
		 *
		 * @param <T>
		 */
		protected interface ValueConverter<T> {

			Object convert(T source, TypeInformation<?> typeHint);

		}

		/**
		 * Converts a container {@code source} value into {@link TypeInformation the target type}. Containers may
		 * recursively apply conversions for entities, collections, maps, etc.
		 *
		 * @param <T>
		 */
		protected interface ContainerValueConverter<T> {

			Object convert(ConversionContext context, T source, TypeInformation<?> typeHint);

		}

	}

	/**
	 * Projecting variant of {@link ConversionContext} applying mapping-metadata rules from the related entity.
	 *
	 * @since 3.2
	 */
	protected class ProjectingConversionContext extends DefaultConversionContext {

		private final EntityProjection<?, ?> returnedTypeDescriptor;

		protected ProjectingConversionContext(RelationalConverter sourceConverter, CustomConversions customConversions,
				ObjectPath path, ContainerValueConverter<Collection<?>> collectionConverter,
				ContainerValueConverter<Map<?, ?>> mapConverter, ValueConverter<Object> elementConverter,
				EntityProjection<?, ?> projection) {
			super(sourceConverter, customConversions, path,
					(context, source, typeHint) -> doReadOrProject(context, source, typeHint, projection),

					collectionConverter, mapConverter, elementConverter);
			this.returnedTypeDescriptor = projection;
		}

		@Override
		public ConversionContext forProperty(String name) {

			EntityProjection<?, ?> property = returnedTypeDescriptor.findProperty(name);
			if (property == null) {
				return new DefaultConversionContext(sourceConverter, conversions, objectPath,
						MappingRelationalConverter.this::readAggregate, collectionConverter, mapConverter, elementConverter);
			}

			return new ProjectingConversionContext(sourceConverter, conversions, objectPath, collectionConverter,
					mapConverter, elementConverter, property);
		}

		@Override
		public ConversionContext withPath(ObjectPath currentPath) {
			return new ProjectingConversionContext(sourceConverter, conversions, currentPath, collectionConverter,
					mapConverter, elementConverter, returnedTypeDescriptor);
		}
	}

	/**
	 * Conversion context defining an interface for graph-traversal-based conversion of row documents. Entrypoint for
	 * recursive conversion of {@link RowDocument} and other types.
	 *
	 * @since 3.2
	 */
	protected interface ConversionContext {

		/**
		 * Converts a source object into {@link TypeInformation target}.
		 *
		 * @param source must not be {@literal null}.
		 * @param typeHint must not be {@literal null}.
		 * @return the converted object.
		 */
		default <S> S convert(Object source, TypeInformation<? extends S> typeHint) {
			return convert(source, typeHint, this);
		}

		/**
		 * Converts a source object into {@link TypeInformation target}.
		 *
		 * @param source must not be {@literal null}.
		 * @param typeHint must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @return the converted object.
		 */
		<S> S convert(Object source, TypeInformation<? extends S> typeHint, ConversionContext context);

		/**
		 * Obtain a {@link ConversionContext} for the given property {@code name}.
		 *
		 * @param name must not be {@literal null}.
		 * @return the {@link ConversionContext} to be used for conversion of the given property.
		 */
		default ConversionContext forProperty(String name) {
			return this;
		}

		/**
		 * Obtain a {@link ConversionContext} for the given {@link RelationalPersistentProperty}.
		 *
		 * @param property must not be {@literal null}.
		 * @return the {@link ConversionContext} to be used for conversion of the given property.
		 */
		default ConversionContext forProperty(RelationalPersistentProperty property) {
			return forProperty(property.getName());
		}

		/**
		 * Create a new {@link ConversionContext} with {@link ObjectPath currentPath} applied.
		 *
		 * @param currentPath must not be {@literal null}.
		 * @return a new {@link ConversionContext} with {@link ObjectPath currentPath} applied.
		 */
		ConversionContext withPath(ObjectPath currentPath);

		/**
		 * @return the current {@link ObjectPath}. Can be {@link ObjectPath#ROOT} for top-level contexts.
		 */
		ObjectPath getPath();

		/**
		 * @return the associated conversions.
		 */
		CustomConversions getCustomConversions();

		/**
		 * @return source {@link RelationalConverter}.
		 */
		RelationalConverter getSourceConverter();

	}

	enum NoOpParameterValueProvider implements ParameterValueProvider<RelationalPersistentProperty> {

		INSTANCE;

		@Override
		public <T> T getParameterValue(Parameter<T, RelationalPersistentProperty> parameter) {
			return null;
		}
	}

	/**
	 * Extended {@link ParameterValueProvider} that can report whether a property value is present and contextualize the
	 * instance for specific behavior like projection mapping in the context of a property.
	 */
	protected interface RelationalPropertyValueProvider extends PropertyValueProvider<RelationalPersistentProperty> {

		/**
		 * Determine whether there is a value for the given {@link RelationalPersistentProperty}.
		 *
		 * @param property the property to check for whether a value is present.
		 */
		boolean hasValue(RelationalPersistentProperty property);

		/**
		 * Determine whether there is a non empty value for the given {@link RelationalPersistentProperty}.
		 *
		 * @param property the property to check for whether a value is present.
		 */
		boolean hasNonEmptyValue(RelationalPersistentProperty property);

		/**
		 * Contextualize this property value provider.
		 *
		 * @param context the context to use.
		 */
		RelationalPropertyValueProvider withContext(ConversionContext context);

	}

	/**
	 * {@link RelationalPropertyValueProvider} extension to obtain values for {@link AggregatePath}s.
	 */
	protected interface AggregatePathValueProvider extends RelationalPropertyValueProvider {

		/**
		 * Determine whether there is a value for the given {@link AggregatePath}.
		 *
		 * @param path the path to check for whether a value is present.
		 */
		boolean hasValue(AggregatePath path);

		boolean hasNonEmptyValue(AggregatePath aggregatePath);

		/**
		 * Determine whether there is a value for the given {@link SqlIdentifier}.
		 *
		 * @param identifier the path to check for whether a value is present.
		 */
		boolean hasValue(SqlIdentifier identifier);

		/**
		 * Return a value for the given {@link AggregatePath}.
		 *
		 * @param path will never be {@literal null}.
		 */
		@Nullable
		Object getValue(AggregatePath path);

		/**
		 * Contextualize this property value provider.
		 */
		@Override
		AggregatePathValueProvider withContext(ConversionContext context);
	}

	/**
	 * {@link PropertyValueProvider} to evaluate a SpEL expression if present on the property or simply accesses the field
	 * of the configured source {@link RowDocument}.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	protected static final class DocumentValueProvider
			implements RelationalPropertyValueProvider, AggregatePathValueProvider {

		private final ConversionContext context;
		private final RowDocumentAccessor accessor;
		private final ValueExpressionEvaluator evaluator;
		private final SpELContext spELContext;
		private final RowDocument document;

		/**
		 * Creates a new {@link RelationalPropertyValueProvider} for the given source and {@link ValueExpressionEvaluator}.
		 *
		 * @param context must not be {@literal null}.
		 * @param accessor must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 */
		private DocumentValueProvider(ConversionContext context, RowDocumentAccessor accessor,
				ValueExpressionEvaluator evaluator, SpELContext spELContext) {

			Assert.notNull(context, "ConversionContext must no be null");
			Assert.notNull(accessor, "DocumentAccessor must no be null");
			Assert.notNull(evaluator, "ValueExpressionEvaluator must not be null");

			this.context = context;
			this.accessor = accessor;
			this.evaluator = evaluator;
			this.spELContext = spELContext;
			this.document = accessor.getDocument();
		}

		@Override
		@Nullable
		@SuppressWarnings("unchecked")
		public <T> T getPropertyValue(RelationalPersistentProperty property) {

			String expression = property.getSpelExpression();
			Object value = expression != null ? evaluator.evaluate(expression) : accessor.get(property);

			if (value == null) {
				return null;
			}

			ConversionContext contextToUse = context.forProperty(property);

			return (T) contextToUse.convert(value, property.getTypeInformation());
		}

		@Override
		public boolean hasValue(RelationalPersistentProperty property) {
			return accessor.hasValue(property);
		}

		@Override
		public boolean hasNonEmptyValue(RelationalPersistentProperty property) {
			return hasValue(property);
		}

		@Nullable
		@Override
		public Object getValue(AggregatePath path) {

			Object value = document.get(path.getColumnInfo().alias().getReference());

			if (value == null) {
				return null;
			}

			return context.convert(value, path.getRequiredLeafProperty().getTypeInformation());
		}

		@Override
		public boolean hasValue(AggregatePath path) {

			Object value = document.get(path.getColumnInfo().alias().getReference());

			if (value == null) {
				return false;
			}

			if (!path.isCollectionLike()) {
				return true;
			}

			return true;
		}

		@Override
		public boolean hasNonEmptyValue(AggregatePath path) {

			if (!hasValue(path)) {
				return false;
			}

			Object value = document.get(path.getColumnInfo().alias().getReference());

			if (value instanceof Collection<?> || value.getClass().isArray()) {
				return !ObjectUtils.isEmpty(value);
			}

			return true;
		}

		@Override
		public boolean hasValue(SqlIdentifier identifier) {
			return document.get(identifier.getReference()) != null;
		}

		@Override
		public DocumentValueProvider withContext(ConversionContext context) {
			return context == this.context ? this : new DocumentValueProvider(context, accessor, evaluator, spELContext);
		}

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

			Assert.notNull(delegate, "Delegate must not be null");

			this.delegate = delegate;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> T getParameterValue(Parameter<T, P> parameter) {
			return (T) readValue(delegate.apply(parameter), parameter.getType());
		}
	}

	/**
	 * Extension of {@link ValueExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 */
	private static class ConverterAwareExpressionParameterValueProvider
			extends ValueExpressionParameterValueProvider<RelationalPersistentProperty> {

		private final ConversionContext context;

		/**
		 * Creates a new {@link ConverterAwareExpressionParameterValueProvider}.
		 *
		 * @param context must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 */
		public ConverterAwareExpressionParameterValueProvider(ConversionContext context, ValueExpressionEvaluator evaluator,
				ConversionService conversionService, ParameterValueProvider<RelationalPersistentProperty> delegate) {

			super(evaluator, conversionService, delegate);

			Assert.notNull(context, "ConversionContext must no be null");

			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertExpressionValue(Object object,
				Parameter<T, RelationalPersistentProperty> parameter) {
			return context.convert(object, parameter.getType());
		}

	}

	private record PropertyTranslatingPropertyAccessor<T>(PersistentPropertyAccessor<T> delegate,
			PersistentPropertyTranslator propertyTranslator) implements PersistentPropertyAccessor<T> {

		static <T> PersistentPropertyAccessor<T> create(PersistentPropertyAccessor<T> delegate,
				PersistentPropertyTranslator propertyTranslator) {
			return new PropertyTranslatingPropertyAccessor<>(delegate, propertyTranslator);
		}

		@Override
		public void setProperty(PersistentProperty<?> property, @Nullable Object value) {
			delegate.setProperty(translate(property), value);
		}

		@Override
		public Object getProperty(PersistentProperty<?> property) {
			return delegate.getProperty(translate(property));
		}

		@Override
		public T getBean() {
			return delegate.getBean();
		}

		private RelationalPersistentProperty translate(PersistentProperty<?> property) {
			return propertyTranslator.translate((RelationalPersistentProperty) property);
		}
	}

}
