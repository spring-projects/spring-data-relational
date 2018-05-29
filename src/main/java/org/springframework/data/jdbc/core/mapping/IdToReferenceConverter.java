/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.support.DefaultRepositoryInvokerFactory;
import org.springframework.data.repository.support.Repositories;
import org.springframework.data.repository.support.RepositoryInvoker;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jens Schauder
 */
public class IdToReferenceConverter implements GenericConverter {

	private final ConversionService conversionService;
	private final Repositories repositories;
	private final DefaultRepositoryInvokerFactory repositoryInvokerFactory;

	@java.beans.ConstructorProperties({ "beans", "conversionService" })
	public IdToReferenceConverter(ListableBeanFactory beans, ConversionService conversionService) {
		this.repositories = new Repositories(beans);
		repositoryInvokerFactory = new DefaultRepositoryInvokerFactory(repositories);
		this.conversionService = conversionService;
	}

	@Override
	public Set<ConvertiblePair> getConvertibleTypes() {

		HashSet<ConvertiblePair> convertiblePairs = new HashSet<>();
		convertiblePairs.add(new ConvertiblePair(Object.class, AggregateReference.class));
		return convertiblePairs;
	}

	@Override
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {

		Class<?> referenceTarget = getReferenceTarget(targetType);

		if (referenceTarget == null) {
			return null;
		}


		final Class<Object> idType = repositories.getEntityInformationFor(referenceTarget).getIdType();

		final RepositoryInvoker invoker = repositoryInvokerFactory.getInvokerFor(referenceTarget);

		return new LazyAggregateReference<>(conversionService.convert(source, idType), invoker);

	}

	static Class<?> getReferenceTarget(TypeDescriptor targetType) {

		if (!AggregateReference.class.isAssignableFrom(targetType.getType())) {
			return null;
		}

		return targetType.getResolvableType().getGeneric(0).getRawClass();
	}

	static Class<?> getEntityClass(Class<?> repositoryClass) {

		final ClassTypeInformation<?> classTypeInformation = ClassTypeInformation.from(repositoryClass);

		TypeInformation ti = classTypeInformation.getTypeArgument(Repository.class, 0);

		return ti != null ? ti.getType() : null;
	}

	static Class<?> getIdClass(Class<?> repositoryClass) {

		final ClassTypeInformation<?> classTypeInformation = ClassTypeInformation.from(repositoryClass);

		TypeInformation ti = classTypeInformation.getTypeArgument(Repository.class, 1);

		return ti != null ? ti.getType() : null;
	}
}
