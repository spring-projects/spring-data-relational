/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.util.Assert;

/**
 * {@link MappingContext} implementation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class RelationalMappingContext
		extends AbstractMappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> {

	private static final Logger logger = LoggerFactory.getLogger(RelationalMappingContext.class);

	private final NamingStrategy namingStrategy;
	private final Map<AggregatePathCacheKey, AggregatePath> aggregatePathCache = new ConcurrentHashMap<>();

	private boolean forceQuote = true;

	private final SqlIdentifierExpressionEvaluator sqlIdentifierExpressionEvaluator = new SqlIdentifierExpressionEvaluator(
			EvaluationContextProvider.DEFAULT);
	private boolean singleQueryLoadingEnabled = false;

	/**
	 * Creates a new {@link RelationalMappingContext}.
	 */
	public RelationalMappingContext() {
		this(new DefaultNamingStrategy());
	}

	/**
	 * Creates a new {@link RelationalMappingContext} using the given {@link NamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public RelationalMappingContext(NamingStrategy namingStrategy) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		this.namingStrategy = new CachingNamingStrategy(namingStrategy);

		setSimpleTypeHolder(SimpleTypeHolder.DEFAULT);
	}

	/**
	 * Return whether quoting should be enabled for all table and column names. Quoting is enabled by default.
	 *
	 * @return
	 * @since 2.0
	 */
	public boolean isForceQuote() {
		return forceQuote;
	}

	/**
	 * Enable/disable quoting for all tables and column names.
	 *
	 * @param forceQuote
	 */
	public void setForceQuote(boolean forceQuote) {
		this.forceQuote = forceQuote;
	}

	/**
	 * Set the {@link SqlIdentifierSanitizer} to sanitize
	 * {@link org.springframework.data.relational.core.sql.SqlIdentifier identifiers} created from SpEL expressions.
	 *
	 * @param sanitizer must not be {@literal null}.
	 * @since 3.2
	 */
	public void setSqlIdentifierSanitizer(SqlIdentifierSanitizer sanitizer) {
		this.sqlIdentifierExpressionEvaluator.setSanitizer(sanitizer);
	}

	public NamingStrategy getNamingStrategy() {
		return this.namingStrategy;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.sqlIdentifierExpressionEvaluator.setProvider(new ExtensionAwareEvaluationContextProvider(applicationContext));
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.sqlIdentifierExpressionEvaluator.setEnvironment(environment);
		super.setEnvironment(environment);
	}

	@Nullable
	@Override
	public RelationalPersistentEntity<?> getPersistentEntity(RelationalPersistentProperty persistentProperty) {

		boolean embeddedDelegation = persistentProperty instanceof EmbeddedRelationalPersistentProperty;

		RelationalPersistentEntity<?> entity = super.getPersistentEntity(persistentProperty);

		if (entity != null && (persistentProperty.isEmbedded() || embeddedDelegation)) {
			return new EmbeddedRelationalPersistentEntity<>(entity, new EmbeddedContext(persistentProperty));
		}

		return entity;
	}

	@Override
	protected <T> RelationalPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		BasicRelationalPersistentEntity<T> entity = new BasicRelationalPersistentEntity<>(typeInformation,
				this.namingStrategy, this.sqlIdentifierExpressionEvaluator);
		entity.setForceQuote(isForceQuote());

		// Validate Set<T> properties in @MappedCollection context
		validateSetMappedCollectionProperties(entity);

		return entity;
	}

	@Override
	protected RelationalPersistentProperty createPersistentProperty(Property property,
			RelationalPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {

		BasicRelationalPersistentProperty persistentProperty = new BasicRelationalPersistentProperty(property, owner,
				simpleTypeHolder, this.namingStrategy);
		applyDefaults(persistentProperty);

		return persistentProperty;
	}

	/**
	 * @return iff single query loading is enabled.
	 * @since 3.2
	 * @see #setSingleQueryLoadingEnabled(boolean)
	 */
	public boolean isSingleQueryLoadingEnabled() {
		return singleQueryLoadingEnabled;
	}

	/**
	 * Set the {@literal singleQueryLoadingEnabled} flag. If it is set to true and the
	 * {@link org.springframework.data.relational.core.dialect.Dialect} supports it, Spring Data JDBC will try to use
	 * Single Query Loading if possible.
	 *
	 * @param singleQueryLoadingEnabled
	 * @since 3.2
	 */
	public void setSingleQueryLoadingEnabled(boolean singleQueryLoadingEnabled) {
		this.singleQueryLoadingEnabled = singleQueryLoadingEnabled;
	}

	protected void applyDefaults(BasicRelationalPersistentProperty persistentProperty) {

		persistentProperty.setForceQuote(isForceQuote());
		persistentProperty.setSqlIdentifierExpressionEvaluator(this.sqlIdentifierExpressionEvaluator);
	}

	/**
	 * Provides an {@link AggregatePath} for the provided {@link PersistentPropertyPath}.
	 *
	 * @param path the path to provide an {@link AggregatePath} for. Must not be null.
	 * @return an {@link AggregatePath} on the provided path.
	 * @since 3.2
	 */
	public AggregatePath getAggregatePath(PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		AggregatePathCacheKey cacheKey = AggregatePathCacheKey.of(path);

		AggregatePath aggregatePath = aggregatePathCache.get(cacheKey);
		if (aggregatePath == null) {

			aggregatePath = new DefaultAggregatePath(this, path);
			aggregatePathCache.put(cacheKey, aggregatePath);
		}

		return aggregatePath;
	}

	public AggregatePath getAggregatePath(RelationalPersistentEntity<?> type) {

		AggregatePathCacheKey cacheKey = AggregatePathCacheKey.of(type);

		AggregatePath aggregatePath = aggregatePathCache.get(cacheKey);

		if (aggregatePath == null) {

			aggregatePath = new DefaultAggregatePath(this, type);
			aggregatePathCache.put(cacheKey, aggregatePath);
		}

		return aggregatePath;
	}

	/**
	 * Validates Set<T> properties in nested @MappedCollection scenarios.
	 * 
	 * @param entity the entity to validate
	 */
	private <T> void validateSetMappedCollectionProperties(RelationalPersistentEntity<T> entity) {
		for (RelationalPersistentProperty property : entity) {
			if (isSetMappedCollection(property)) {
				validateSetMappedCollectionProperty(property);
			}
		}
	}

	/**
	 * Checks if a property is a Set with @MappedCollection annotation.
	 */
	private boolean isSetMappedCollection(RelationalPersistentProperty property) {
		return property.isCollectionLike() 
			&& Set.class.isAssignableFrom(property.getType())
			&& property.isAnnotationPresent(MappedCollection.class);
	}

	/**
	 * Validates a Set<T> property in @MappedCollection context.
	 * 
	 * @param property the Set property to validate
	 */
	private void validateSetMappedCollectionProperty(RelationalPersistentProperty property) {
		Class<?> elementType = property.getComponentType();
		if (elementType == null) {
			return;
		}
		
		RelationalPersistentEntity<?> elementEntity = getPersistentEntity(elementType);
		if (elementEntity == null) {
			return;
		}
		
		boolean hasId = elementEntity.hasIdProperty();
		boolean hasEntityOrCollectionReferences = hasEntityOrCollectionReferences(elementEntity);
		
		if (!hasId && hasEntityOrCollectionReferences) {
			String message = String.format(
				"Invalid @MappedCollection usage: Set<%s> in %s.%s. " +
				"Set elements without @Id must not contain entity or collection references. " +
				"Consider using List instead or add @Id to %s.",
				elementType.getSimpleName(),
				property.getOwner().getType().getSimpleName(),
				property.getName(),
				elementType.getSimpleName()
			);
			
			logger.warn(message);
		}
	}

	/**
	 * Checks if an entity has any properties that are entities or collections.
	 */
	private boolean hasEntityOrCollectionReferences(RelationalPersistentEntity<?> entity) {
		for (RelationalPersistentProperty prop : entity) {
			if (prop.isIdProperty() || prop.isVersionProperty()) {
				continue;
			}
			
			if (prop.isEntity() || prop.isCollectionLike()) {
				return true;
			}
		}
		return false;
	}

	private record AggregatePathCacheKey(RelationalPersistentEntity<?> root,
			@Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		/**
		 * Create a new AggregatePathCacheKey for a root entity.
		 *
		 * @param root the root entity.
		 */
		static AggregatePathCacheKey of(RelationalPersistentEntity<?> root) {
			return new AggregatePathCacheKey(root, null);
		}

		/**
		 * Create a new AggregatePathCacheKey for a property path.
		 *
		 * @param path {@Literal AggregatePath} to obtain a cache key for.
		 */
		static AggregatePathCacheKey of(PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
			return new AggregatePathCacheKey(path.getBaseProperty().getOwner(), path);
		}
	}
}
