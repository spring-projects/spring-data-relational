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
package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Annotation;
import java.util.Iterator;

import org.springframework.core.env.Environment;
import org.springframework.data.mapping.*;
import org.springframework.data.mapping.model.PersistentPropertyAccessorFactory;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.Streamable;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Embedded entity extension for a {@link Embedded entity}.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class EmbeddedRelationalPersistentEntity<T> implements RelationalPersistentEntity<T> {

	private final RelationalPersistentEntity<T> delegate;

	private final EmbeddedContext context;

	public EmbeddedRelationalPersistentEntity(RelationalPersistentEntity<T> delegate, EmbeddedContext context) {
		this.delegate = delegate;
		this.context = context;
	}

	@Override
	public SqlIdentifier getTableName() {
		throw new MappingException("Cannot map embedded entity to table");
	}

	@Override
	public SqlIdentifier getIdColumn() {
		throw new MappingException("Embedded entity does not have an id column");
	}

	@Override
	public void addPersistentProperty(RelationalPersistentProperty property) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addAssociation(Association<RelationalPersistentProperty> association) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void verify() throws MappingException {}

	@Override
	public Iterator<RelationalPersistentProperty> iterator() {

		Iterator<RelationalPersistentProperty> iterator = delegate.iterator();

		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public RelationalPersistentProperty next() {
				return wrap(iterator.next());
			}
		};
	}

	@Override
	public void setPersistentPropertyAccessorFactory(PersistentPropertyAccessorFactory factory) {
		delegate.setPersistentPropertyAccessorFactory(factory);
	}

	@Override
	public void setEnvironment(Environment environment) {
		delegate.setEnvironment(environment);
	}

	@Override
	public void setEvaluationContextProvider(EvaluationContextProvider provider) {
		delegate.setEvaluationContextProvider(provider);
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	@Deprecated
	@Nullable
	public PreferredConstructor<T, RelationalPersistentProperty> getPersistenceConstructor() {
		return delegate.getPersistenceConstructor();
	}

	@Override
	@Nullable
	public InstanceCreatorMetadata<RelationalPersistentProperty> getInstanceCreatorMetadata() {
		return delegate.getInstanceCreatorMetadata();
	}

	@Override
	public boolean isCreatorArgument(PersistentProperty<?> property) {
		return delegate.isCreatorArgument(property);
	}

	@Override
	public boolean isIdProperty(PersistentProperty<?> property) {
		return delegate.isIdProperty(property);
	}

	@Override
	public boolean isVersionProperty(PersistentProperty<?> property) {
		return delegate.isVersionProperty(property);
	}

	@Override
	@Nullable
	public RelationalPersistentProperty getIdProperty() {
		return wrap(delegate.getIdProperty());
	}

	@Override
	@Nullable
	public RelationalPersistentProperty getVersionProperty() {
		return wrap(delegate.getVersionProperty());
	}

	@Override
	@Nullable
	public RelationalPersistentProperty getPersistentProperty(String name) {
		return wrap(delegate.getPersistentProperty(name));
	}

	@Override
	public Iterable<RelationalPersistentProperty> getPersistentProperties(Class<? extends Annotation> annotationType) {
		return Streamable.of(delegate.getPersistentProperties(annotationType)).map(this::wrap);
	}

	@Override
	public boolean hasIdProperty() {
		return delegate.hasIdProperty();
	}

	@Override
	public boolean hasVersionProperty() {
		return delegate.hasVersionProperty();
	}

	@Override
	public Class<T> getType() {
		return delegate.getType();
	}

	@Override
	public Alias getTypeAlias() {
		return delegate.getTypeAlias();
	}

	@Override
	public TypeInformation<T> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	public void doWithProperties(PropertyHandler<RelationalPersistentProperty> handler) {
		delegate.doWithProperties((PropertyHandler<RelationalPersistentProperty>) persistentProperty -> {
			handler.doWithPersistentProperty(wrap(persistentProperty));
		});
	}

	@Override
	public void doWithProperties(SimplePropertyHandler handler) {
		delegate.doWithProperties((SimplePropertyHandler) property -> handler
				.doWithPersistentProperty(wrap((RelationalPersistentProperty) property)));
	}

	@Override
	public void doWithAssociations(AssociationHandler<RelationalPersistentProperty> handler) {
		delegate.doWithAssociations((AssociationHandler<RelationalPersistentProperty>) association -> {
			handler.doWithAssociation(new Association<>(wrap(association.getInverse()), wrap(association.getObverse())));
		});
	}

	@Override
	public void doWithAssociations(SimpleAssociationHandler handler) {
		delegate.doWithAssociations((AssociationHandler<RelationalPersistentProperty>) association -> {
			handler.doWithAssociation(new Association<>(wrap(association.getInverse()), wrap(association.getObverse())));
		});
	}

	@Override
	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	@Override
	public <A extends Annotation> boolean isAnnotationPresent(Class<A> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	@Override
	public <B> PersistentPropertyAccessor<B> getPropertyAccessor(B bean) {
		return delegate.getPropertyAccessor(bean);
	}

	@Override
	public <B> PersistentPropertyPathAccessor<B> getPropertyPathAccessor(B bean) {
		return delegate.getPropertyPathAccessor(bean);
	}

	@Override
	public IdentifierAccessor getIdentifierAccessor(Object bean) {
		return delegate.getIdentifierAccessor(bean);
	}

	@Override
	public boolean isNew(Object bean) {
		return delegate.isNew(bean);
	}

	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean requiresPropertyPopulation() {
		return delegate.requiresPropertyPopulation();
	}

	@Nullable
	private RelationalPersistentProperty wrap(@Nullable RelationalPersistentProperty source) {

		if (source == null) {
			return null;
		}
		return new EmbeddedRelationalPersistentProperty(source, context);
	}

	@Override
	public String toString() {
		return String.format("EmbeddedRelationalPersistentEntity<%s>", getType());
	}
}
