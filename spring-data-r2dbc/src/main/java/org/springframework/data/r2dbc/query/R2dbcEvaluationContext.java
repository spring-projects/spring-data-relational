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
package org.springframework.data.r2dbc.query;

import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.EvaluationContextSupport;
import org.springframework.data.relational.core.query.QueryExpression;
import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.r2dbc.core.binding.MutableBindings;

/**
 * R2DBC-specific {@link QueryExpression.EvaluationContext} implementation.
 *
 * @author Mark Paluch
 */
class R2dbcEvaluationContext extends EvaluationContextSupport {

	private final QueryExpression.ExpressionTypeContext type;
	private final MutableBindings bindings;

	public R2dbcEvaluationContext(Table table, RelationalConverter converter,
			@Nullable RelationalPersistentEntity<?> entity, MutableBindings bindings) {

		super(table, converter, entity);

		this.type = QueryExpression.ExpressionTypeContext.object();
		this.bindings = bindings;
	}

	public R2dbcEvaluationContext(R2dbcEvaluationContext previous, QueryExpression.ExpressionTypeContext type) {

		super(previous);

		this.type = type;
		this.bindings = previous.bindings;
	}

	@Override
	public QueryExpression.EvaluationContext withType(QueryExpression.ExpressionTypeContext type) {
		return new R2dbcEvaluationContext(this, type);
	}

	@Override
	public BindMarker bind(Object value) {

		Object valueToUse = getConverter().writeValue(value, type.getTargetType());
		return new BindMarkerAdapter(bindings.bind(valueToUse));
	}

	@Override
	public BindMarker bind(String name, Object value) {

		Object valueToUse = getConverter().writeValue(value, type.getTargetType());
		org.springframework.r2dbc.core.binding.BindMarker bindMarker = bindings.nextMarker(name);
		bindings.bind(bindMarker, valueToUse);
		return new BindMarkerAdapter(bindMarker);
	}

	private static class BindMarkerAdapter extends BindMarker
			implements org.springframework.r2dbc.core.binding.BindMarker {

		private final org.springframework.r2dbc.core.binding.BindMarker bindMarker;

		public BindMarkerAdapter(org.springframework.r2dbc.core.binding.BindMarker bindMarker) {
			this.bindMarker = bindMarker;
		}

		@Override
		public String getPlaceholder() {
			return bindMarker.getPlaceholder();
		}

		@Override
		public void bind(BindTarget bindTarget, Object value) {
			bindMarker.bind(bindTarget, value);
		}

		@Override
		public void bindNull(BindTarget bindTarget, Class<?> valueType) {
			bindMarker.bindNull(bindTarget, valueType);
		}

		@Override
		public String toString() {
			return "[" + getPlaceholder() + "]";
		}
	}
}
