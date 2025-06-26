/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.relational.core.query;

import org.springframework.data.relational.core.binding.BindMarkers;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Table;

/**
 * @author Mark Paluch
 */
public class SimpleEvaluationContext extends EvaluationContextSupport {

	private final BindMarkers bindMarkers;
	private final Bindings bindings;
	private final QueryExpression.ExpressionTypeContext type;

	public SimpleEvaluationContext(Table table, BindMarkers bindMarkers, Bindings bindings, RelationalConverter converter,
			RelationalPersistentEntity<?> entity) {

		super(table, converter, entity);
		this.bindMarkers = bindMarkers;
		this.bindings = bindings;
		this.type = QueryExpression.ExpressionTypeContext.object();

	}

	public SimpleEvaluationContext(SimpleEvaluationContext previous, QueryExpression.ExpressionTypeContext type) {
		super(previous);
		this.bindMarkers = previous.bindMarkers;
		this.bindings = previous.bindings;
		this.type = type;
	}

	public Bindings getBindings() {
		return bindings;
	}

	@Override
	public QueryExpression.EvaluationContext withType(QueryExpression.ExpressionTypeContext type) {
		return new SimpleEvaluationContext(this, type);
	}

	@Override
	public BindMarker bind(Object value) {

		BindMarker bindMarker = bindMarkers.next();

		bindings.bind(bindMarker, getConverter().writeValue(value, type.getTargetType()));

		return bindMarker;
	}

	@Override
	public BindMarker bind(String name, Object value) {

		BindMarker bindMarker = bindMarkers.next(name);

		bindings.bind(bindMarker, getConverter().writeValue(value, type.getTargetType()));

		return bindMarker;
	}

}
