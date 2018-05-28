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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jens Schauder
 */
@RequiredArgsConstructor
public class IdToReferenceConverter implements GenericConverter {

	@NonNull final ListableBeanFactory beans;

	@Override
	public Set<ConvertiblePair> getConvertibleTypes() {

		HashSet<ConvertiblePair> convertiblePairs = new HashSet<>();
		convertiblePairs.add(new ConvertiblePair(Object.class, Reference.class));
		return convertiblePairs;
	}

	@Override
	public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {

		Class referenceTarget = getReferenceTarget(targetType);

		if (referenceTarget == null) {
			return null;
		}

		Map<String, CrudRepository> beansOfType = beans.getBeansOfType(CrudRepository.class);

		for (CrudRepository repository : beansOfType.values()) {
			final Class<?> entityClass = getEntityClass(repository.getClass());

			if (entityClass == referenceTarget) {
				return Reference.to(source, repository);
			}
		}

		return null;
	}

	static Class<?> getReferenceTarget(TypeDescriptor targetType) {

		if (!Reference.class.isAssignableFrom(targetType.getType())) {
			return null;
		}

		return targetType.getResolvableType().getGeneric(0).getRawClass();
	}

	static Class<?> getEntityClass(Class<?> repositoryClass) {

		final ClassTypeInformation<?> classTypeInformation = ClassTypeInformation.from(repositoryClass);

		TypeInformation ti = classTypeInformation.getTypeArgument(Repository.class, 0);

		return ti.getType();

	}
}
