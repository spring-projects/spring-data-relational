/*
 * Copyright 2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.InstanceCreatorMetadata;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link RelationalConverter} that uses a {@link MappingContext} to apply sophisticated mapping of domain objects from
 * {@link RowDocument}.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public class MappingRelationalConverter extends BasicRelationalConverter {

	private SpELContext spELContext;

	/**
	 * Creates a new {@link MappingRelationalConverter} given the new {@link RelationalMappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public MappingRelationalConverter(RelationalMappingContext context) {
		super(context);
		this.spELContext = new SpELContext(DocumentPropertyAccessor.INSTANCE);
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

	/**
	 * Read a {@link RowDocument} into the requested {@link Class aggregate type}.
	 *
	 * @param type target aggregate type.
	 * @param source source {@link RowDocument}.
	 * @return the converted object.
	 * @param <R> aggregate type.
	 */
	public <R> R read(Class<R> type, RowDocument source) {
		return read(TypeInformation.of(type), source);
	}

	protected <S extends Object> S read(TypeInformation<S> type, RowDocument source) {
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
	@SuppressWarnings("unchecked")
	protected <S extends Object> S readAggregate(ConversionContext context, RowDocument document,
			TypeInformation<? extends S> typeHint) {

		Class<? extends S> rawType = typeHint.getType();

		if (getConversions().hasCustomReadTarget(document.getClass(), rawType)) {
			return doConvert(document, rawType, typeHint.getType());
		}

		if (RowDocument.class.isAssignableFrom(rawType)) {
			return (S) document;
		}

		if (typeHint.isMap()) {
			return context.convert(document, typeHint);
		}

		RelationalPersistentEntity<?> entity = getMappingContext().getPersistentEntity(rawType);

		if (entity == null) {
			throw new MappingException(
					String.format("Expected to read Document %s into type %s but didn't find a PersistentEntity for the latter",
							document, rawType));
		}

		return read(context, (RelationalPersistentEntity<S>) entity, document);
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
	@SuppressWarnings("unchecked")
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

	private <T extends Object> T doConvert(Object value, Class<? extends T> target) {
		return doConvert(value, target, null);
	}

	@SuppressWarnings("ConstantConditions")
	private <T extends Object> T doConvert(Object value, Class<? extends T> target,
			@Nullable Class<? extends T> fallback) {

		if (getConversionService().canConvert(value.getClass(), target) || fallback == null) {
			return getConversionService().convert(value, target);
		}
		return getConversionService().convert(value, fallback);
	}

	private <S> S read(ConversionContext context, RelationalPersistentEntity<S> entity, RowDocument document) {

		SpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(document, spELContext);
		RowDocumentAccessor documentAccessor = new RowDocumentAccessor(document);

		InstanceCreatorMetadata<RelationalPersistentProperty> instanceCreatorMetadata = entity.getInstanceCreatorMetadata();

		ParameterValueProvider<RelationalPersistentProperty> provider = instanceCreatorMetadata != null
				&& instanceCreatorMetadata.hasParameters() ? getParameterProvider(context, entity, documentAccessor, evaluator)
						: NoOpParameterValueProvider.INSTANCE;

		EntityInstantiator instantiator = getEntityInstantiators().getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, provider);

		if (entity.requiresPropertyPopulation()) {
			return populateProperties(context, entity, documentAccessor, evaluator, instance);
		}

		return instance;
	}

	private ParameterValueProvider<RelationalPersistentProperty> getParameterProvider(ConversionContext context,
			RelationalPersistentEntity<?> entity, RowDocumentAccessor source, SpELExpressionEvaluator evaluator) {

		RelationalPropertyValueProvider provider = new RelationalPropertyValueProvider(context, source, evaluator,
				spELContext);

		// TODO: Add support for enclosing object (non-static inner classes)
		PersistentEntityParameterValueProvider<RelationalPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<>(
				entity, provider, context.getPath().getCurrentObject());

		return new ConverterAwareSpELExpressionParameterValueProvider(context, evaluator, getConversionService(),
				parameterProvider);
	}

	private <S> S populateProperties(ConversionContext context, RelationalPersistentEntity<S> entity,
			RowDocumentAccessor documentAccessor, SpELExpressionEvaluator evaluator, S instance) {

		PersistentPropertyAccessor<S> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(instance),
				getConversionService());

		// Make sure id property is set before all other properties
		ObjectPath currentPath = context.getPath().push(accessor.getBean(), entity);
		ConversionContext contextToUse = context.withPath(currentPath);

		RelationalPropertyValueProvider valueProvider = new RelationalPropertyValueProvider(contextToUse, documentAccessor,
				evaluator, spELContext);

		Predicate<RelationalPersistentProperty> propertyFilter = isConstructorArgument(entity).negate();
		readProperties(contextToUse, entity, accessor, documentAccessor, valueProvider, evaluator, propertyFilter);

		return accessor.getBean();
	}

	private void readProperties(ConversionContext context, RelationalPersistentEntity<?> entity,
			PersistentPropertyAccessor<?> accessor, RowDocumentAccessor documentAccessor,
			RelationalPropertyValueProvider valueProvider, SpELExpressionEvaluator evaluator,
			Predicate<RelationalPersistentProperty> propertyFilter) {

		for (RelationalPersistentProperty prop : entity) {

			if (!propertyFilter.test(prop)) {
				continue;
			}

			ConversionContext propertyContext = context.forProperty(prop);
			RelationalPropertyValueProvider valueProviderToUse = valueProvider.withContext(propertyContext);

			if (prop.isAssociation()) {

				// TODO: Read AggregateReference
				continue;
			}

			if (prop.isEmbedded()) {
				accessor.setProperty(prop, readEmbedded(propertyContext, documentAccessor, prop,
						getMappingContext().getRequiredPersistentEntity(prop)));
				continue;
			}

			if (!documentAccessor.hasValue(prop)) {
				continue;
			}

			accessor.setProperty(prop, valueProviderToUse.getPropertyValue(prop));
		}
	}

	@Nullable
	private Object readEmbedded(ConversionContext context, RowDocumentAccessor documentAccessor,
			RelationalPersistentProperty prop, RelationalPersistentEntity<?> unwrappedEntity) {

		if (prop.findAnnotation(Embedded.class).onEmpty().equals(OnEmpty.USE_EMPTY)) {
			return read(context, unwrappedEntity, documentAccessor.getDocument());
		}

		for (RelationalPersistentProperty persistentProperty : unwrappedEntity) {
			if (documentAccessor.hasValue(persistentProperty)) {
				return read(context, unwrappedEntity, documentAccessor.getDocument());
			}
		}

		return null;
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

		DefaultConversionContext(RelationalConverter sourceConverter,
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
		public <S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint,
				ConversionContext context) {

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
		interface ValueConverter<T> {

			Object convert(T source, TypeInformation<?> typeHint);

		}

		/**
		 * Converts a container {@code source} value into {@link TypeInformation the target type}. Containers may
		 * recursively apply conversions for entities, collections, maps, etc.
		 *
		 * @param <T>
		 */
		interface ContainerValueConverter<T> {

			Object convert(ConversionContext context, T source, TypeInformation<?> typeHint);

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
		default <S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint) {
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
		<S extends Object> S convert(Object source, TypeInformation<? extends S> typeHint, ConversionContext context);

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

		ObjectPath getPath();

		CustomConversions getCustomConversions();

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
	 * {@link PropertyValueProvider} to evaluate a SpEL expression if present on the property or simply accesses the field
	 * of the configured source {@link RowDocument}.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	record RelationalPropertyValueProvider(ConversionContext context, RowDocumentAccessor accessor,
			SpELExpressionEvaluator evaluator,
			SpELContext spELContext) implements PropertyValueProvider<RelationalPersistentProperty> {

		/**
		 * Creates a new {@link RelationalPropertyValueProvider} for the given source and {@link SpELExpressionEvaluator}.
		 *
		 * @param context must not be {@literal null}.
		 * @param accessor must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 */
		RelationalPropertyValueProvider {

			Assert.notNull(context, "ConversionContext must no be null");
			Assert.notNull(accessor, "DocumentAccessor must no be null");
			Assert.notNull(evaluator, "SpELExpressionEvaluator must not be null");
		}

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

		public RelationalPropertyValueProvider withContext(ConversionContext context) {

			return context == this.context ? this
					: new RelationalPropertyValueProvider(context, accessor, evaluator, spELContext);
		}
	}

	/**
	 * Extension of {@link SpELExpressionParameterValueProvider} to recursively trigger value conversion on the raw
	 * resolved SpEL value.
	 */
	private static class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<RelationalPersistentProperty> {

		private final ConversionContext context;

		/**
		 * Creates a new {@link ConverterAwareSpELExpressionParameterValueProvider}.
		 *
		 * @param context must not be {@literal null}.
		 * @param evaluator must not be {@literal null}.
		 * @param conversionService must not be {@literal null}.
		 * @param delegate must not be {@literal null}.
		 */
		public ConverterAwareSpELExpressionParameterValueProvider(ConversionContext context,
				SpELExpressionEvaluator evaluator, ConversionService conversionService,
				ParameterValueProvider<RelationalPersistentProperty> delegate) {

			super(evaluator, conversionService, delegate);

			Assert.notNull(context, "ConversionContext must no be null");

			this.context = context;
		}

		@Override
		protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, RelationalPersistentProperty> parameter) {
			return context.convert(object, parameter.getType());
		}
	}

}
