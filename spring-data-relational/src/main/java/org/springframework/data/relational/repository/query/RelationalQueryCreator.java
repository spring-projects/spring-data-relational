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
package org.springframework.data.relational.repository.query;

import java.util.Collection;
import java.util.Iterator;

import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link AbstractQueryCreator} that creates a query from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class RelationalQueryCreator<T> extends AbstractQueryCreator<T, Criteria> {

	private final CriteriaFactory criteriaFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link RelationalEntityMetadata} and
	 * {@link ParameterMetadataProvider}.
	 *
	 * @param tree part tree, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 */
	public RelationalQueryCreator(PartTree tree, RelationalParameterAccessor accessor) {

		super(tree);

		Assert.notNull(accessor, "RelationalParameterAccessor must not be null");
		this.criteriaFactory = new CriteriaFactory(new ParameterMetadataProvider(accessor));
	}

	/**
	 * Creates {@link Criteria} for the given method name part.
	 *
	 * @param part method name part, must not be {@literal null}.
	 * @param iterator iterator over query parameter values
	 * @return new instance of {@link Criteria}
	 */
	@Override
	protected Criteria create(Part part, Iterator<Object> iterator) {
		return criteriaFactory.createCriteria(part);
	}

	/**
	 * Combines the given {@link Criteria} with the new one created for the given method name part using {@code AND}.
	 *
	 * @param part method name part, must not be {@literal null}.
	 * @param base {@link Criteria} to be combined, must not be {@literal null}.
	 * @param iterator iterator over query parameter values
	 * @return {@link Criteria} combination
	 */
	@Override
	protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {
		return base.and(criteriaFactory.createCriteria(part));
	}

	/**
	 * Combines two {@link Criteria}s using {@code OR}.
	 *
	 * @param base {@link Criteria} to be combined, must not be {@literal null}.
	 * @param criteria another {@link Criteria} to be combined, must not be {@literal null}.
	 * @return {@link Criteria} combination
	 */
	@Override
	protected Criteria or(Criteria base, Criteria criteria) {
		return base.or(criteria);
	}

	/**
	 * Validate parameters for the derived query. Specifically checking that the query method defines scalar parameters
	 * and collection parameters where required and that invalid parameter declarations are rejected.
	 *
	 * @param tree
	 * @param parameters
	 */
	public static void validate(PartTree tree, Parameters<?, ?> parameters) {

		int argCount = 0;

		Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();
		for (Part part : parts) {

			int numberOfArguments = part.getNumberOfArguments();
			for (int i = 0; i < numberOfArguments; i++) {

				throwExceptionOnArgumentMismatch(part, parameters, argCount);
				argCount++;
			}
		}
	}

	private static void throwExceptionOnArgumentMismatch(Part part, Parameters<?, ?> parameters, int index) {

		Part.Type type = part.getType();
		String property = part.getProperty().toDotPath();

		if (!parameters.getBindableParameters().hasParameterAt(index)) {

			String msgTemplate = "Query method expects at least %d arguments but only found %d. "
					+ "This leaves an operator of type %s for property %s unbound.";
			String formattedMsg = String.format(msgTemplate, index + 1, index, type.name(), property);
			throw new IllegalStateException(formattedMsg);
		}

		Parameter parameter = parameters.getBindableParameter(index);
		if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {

			String message = wrongParameterTypeMessage(property, type, "Collection", parameter);
			throw new IllegalStateException(message);
		} else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {

			String message = wrongParameterTypeMessage(property, type, "scalar", parameter);
			throw new IllegalStateException(message);
		}
	}

	private static boolean expectsCollection(Part.Type type) {
		return type == Part.Type.IN || type == Part.Type.NOT_IN;
	}

	private static boolean parameterIsCollectionLike(Parameter parameter) {
		return parameter.getType().isArray() || Collection.class.isAssignableFrom(parameter.getType());
	}

	private static boolean parameterIsScalarLike(Parameter parameter) {
		return !Collection.class.isAssignableFrom(parameter.getType());
	}

	private static String wrongParameterTypeMessage(String property, Part.Type operatorType, String expectedArgumentType,
			Parameter parameter) {
		return String.format("Operator %s on %s requires a %s argument, found %s", operatorType.name(), property,
				expectedArgumentType, parameter.getType());
	}
}
