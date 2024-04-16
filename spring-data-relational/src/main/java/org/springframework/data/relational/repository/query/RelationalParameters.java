/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import java.util.List;
import java.util.function.Function;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.util.TypeInformation;

/**
 * Custom extension of {@link Parameters}.
 *
 * @author Mark Paluch
 */
public class RelationalParameters extends Parameters<RelationalParameters, RelationalParameter> {

	/**
	 * Creates a new {@link RelationalParameters} instance from the given {@link ParametersSource}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 */
	public RelationalParameters(ParametersSource parametersSource) {
		super(parametersSource,
				methodParameter -> new RelationalParameter(methodParameter, parametersSource.getDomainTypeInformation()));
	}

	protected RelationalParameters(ParametersSource parametersSource,
			Function<MethodParameter, RelationalParameter> parameterFactory) {
		super(parametersSource, parameterFactory);
	}

	protected RelationalParameters(List<RelationalParameter> parameters) {
		super(parameters);
	}

	@Override
	protected RelationalParameters createFrom(List<RelationalParameter> parameters) {
		return new RelationalParameters(parameters);
	}

	/**
	 * Custom {@link Parameter} implementation.
	 *
	 * @author Mark Paluch
	 * @author Chirag Tailor
	 */
	public static class RelationalParameter extends Parameter {

		private final TypeInformation<?> typeInformation;

		/**
		 * Creates a new {@link RelationalParameter}.
		 *
		 * @param parameter must not be {@literal null}.
		 */
		protected RelationalParameter(MethodParameter parameter, TypeInformation<?> domainType) {
			super(parameter, domainType);
			this.typeInformation = TypeInformation.fromMethodParameter(parameter);

		}

		public ResolvableType getResolvableType() {
			return getTypeInformation().toTypeDescriptor().getResolvableType();
		}

		public TypeInformation<?> getTypeInformation() {
			return typeInformation;
		}
	}
}
