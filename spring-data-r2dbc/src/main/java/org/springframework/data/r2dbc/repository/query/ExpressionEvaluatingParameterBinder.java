/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.BindTargetBinder;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * {@link ExpressionEvaluatingParameterBinder} allows to evaluate, convert and bind parameters to placeholders within a
 * {@link String}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ExpressionEvaluatingParameterBinder {

	private final ExpressionQuery expressionQuery;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final Map<String, Boolean> namedParameters = new ConcurrentHashMap<>();

	/**
	 * Creates new {@link ExpressionEvaluatingParameterBinder}
	 *
	 * @param expressionQuery must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 */
	ExpressionEvaluatingParameterBinder(ExpressionQuery expressionQuery, ReactiveDataAccessStrategy dataAccessStrategy) {
		this.expressionQuery = expressionQuery;
		this.dataAccessStrategy = dataAccessStrategy;
	}

	/**
	 * Bind values provided by {@link RelationalParameterAccessor} to placeholders in {@link ExpressionQuery} while
	 * considering potential conversions and parameter types.
	 *
	 * @param bindTarget must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @param evaluator must not be {@literal null}.
	 */
	void bind(BindTarget bindTarget,
			RelationalParameterAccessor parameterAccessor, ValueEvaluationContext evaluationContext) {

		Object[] values = parameterAccessor.getValues();
		Parameters<?, ?> bindableParameters = parameterAccessor.getBindableParameters();

		bindExpressions(bindTarget, evaluationContext);
		bindParameters(bindTarget, parameterAccessor.hasBindableNullValue(), values, bindableParameters);
	}

	private void bindExpressions(BindTarget bindSpec,
			ValueEvaluationContext evaluationContext) {

		BindTargetBinder binder = new BindTargetBinder(bindSpec);

		expressionQuery.getBindings().forEach((paramName, valueExpression) -> {

			org.springframework.r2dbc.core.Parameter valueForBinding = getBindValue(
					evaluate(valueExpression, evaluationContext));

			binder.bind(paramName, valueForBinding);
		});
	}

	private org.springframework.r2dbc.core.Parameter evaluate(ValueExpression expression,
			ValueEvaluationContext context) {

		Object value = expression.evaluate(context);
		Class<?> valueType = value != null ? value.getClass() : null;

		if (valueType == null) {
			valueType = expression.getValueType(context);
		}

		return org.springframework.r2dbc.core.Parameter.fromOrEmpty(value, valueType == null ? Object.class : valueType);
	}

	private void bindParameters(BindTarget bindSpec,
			boolean hasBindableNullValue, Object[] values, Parameters<?, ?> bindableParameters) {

		int bindingIndex = 0;

		BindTargetBinder binder = new BindTargetBinder(bindSpec);
		for (Parameter bindableParameter : bindableParameters) {

			Optional<String> name = bindableParameter.getName();

			if (name.isPresent() && (isNamedParameterReferencedFromQuery(name)) || !expressionQuery.getBindings().isEmpty()) {

				if (!isNamedParameterReferencedFromQuery(name)) {
					continue;
				}

				org.springframework.r2dbc.core.Parameter parameter = getBindValue(values, bindableParameter);

				if (!parameter.isEmpty() || hasBindableNullValue) {
					binder.bind(name.get(), parameter);
				}

				// skip unused named parameters if there is SpEL
			} else {

				org.springframework.r2dbc.core.Parameter parameter = getBindValue(values, bindableParameter);

				if (!parameter.isEmpty() || hasBindableNullValue) {
					binder.bind(bindingIndex++, parameter);
				}
			}
		}
	}

	private org.springframework.r2dbc.core.Parameter getBindValue(Object[] values, Parameter bindableParameter) {

		org.springframework.r2dbc.core.Parameter parameter = org.springframework.r2dbc.core.Parameter
				.fromOrEmpty(values[bindableParameter.getIndex()], bindableParameter.getType());

		return dataAccessStrategy.getBindValue(parameter);
	}

	private org.springframework.r2dbc.core.Parameter getBindValue(org.springframework.r2dbc.core.Parameter bindValue) {
		return dataAccessStrategy.getBindValue(bindValue);
	}

	private boolean isNamedParameterReferencedFromQuery(Optional<String> name) {

		if (!name.isPresent()) {
			return false;
		}

		return namedParameters.computeIfAbsent(name.get(), it -> {

			Pattern namedParameterPattern = Pattern.compile("(\\W)[:#$@]" + Pattern.quote(it) + "(\\W|$)");
			return namedParameterPattern.matcher(expressionQuery.getQuery()).find();
		});
	}

}
