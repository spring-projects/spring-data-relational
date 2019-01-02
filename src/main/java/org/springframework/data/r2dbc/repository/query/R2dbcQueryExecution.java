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
package org.springframework.data.r2dbc.repository.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.function.FetchSpec;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.repository.query.DtoInstantiatingConverter;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.ClassUtils;

/**
 * Set of classes to contain query execution strategies. Depending (mostly) on the return type of a
 * {@link org.springframework.data.repository.query.QueryMethod}.
 *
 * @author Mark Paluch
 */
interface R2dbcQueryExecution {

	Object execute(FetchSpec<?> query, Class<?> type, String tableName);

	/**
	 * An {@link R2dbcQueryExecution} that wraps the results of the given delegate with the given result processing.
	 */
	@RequiredArgsConstructor
	final class ResultProcessingExecution implements R2dbcQueryExecution {

		private final @NonNull R2dbcQueryExecution delegate;
		private final @NonNull Converter<Object, Object> converter;

		/* (non-Javadoc)
		 * @see org.springframework.data.jdbc.repository.query.R2dbcQueryExecution#execute(org.springframework.data.jdbc.core.function.FetchSpec, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(FetchSpec<?> query, Class<?> type, String tableName) {
			return converter.convert(delegate.execute(query, type, tableName));
		}
	}

	/**
	 * A {@link Converter} to post-process all source objects using the given {@link ResultProcessor}.
	 */
	@RequiredArgsConstructor
	final class ResultProcessingConverter implements Converter<Object, Object> {

		private final @NonNull ResultProcessor processor;
		private final @NonNull MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
		private final @NonNull EntityInstantiators instantiators;

		/* (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Object convert(Object source) {

			ReturnedType returnedType = processor.getReturnedType();

			if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
					mappingContext, instantiators);

			return processor.processResult(source, converter);
		}
	}
}
