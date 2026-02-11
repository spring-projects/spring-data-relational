/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Callback for generating identifier values through a database sequence.
 *
 * @author Mikhail Polivakha
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 3.5
 */
public class IdGeneratingEntityCallback implements BeforeSaveCallback<Object> {

	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context;
	private final SequenceEntityCallbackDelegate delegate;

	public IdGeneratingEntityCallback(
			MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context, Dialect dialect,
			NamedParameterJdbcOperations operations) {

		this.context = context;
		this.delegate = new SequenceEntityCallbackDelegate(dialect, operations);
	}

	@Override
	public Object onBeforeSave(Object aggregate, MutableAggregateChange<Object> aggregateChange) {

		Assert.notNull(aggregate, "Aggregate must not be null");

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(aggregate.getClass());

		RelationalPersistentProperty idProperty = entity.getIdProperty();
		if (idProperty == null || !idProperty.hasSequence()) {
			return aggregate;
		}

		PersistentPropertyAccessor<Object> accessor = entity.getPropertyAccessor(aggregate);
		if (delegate.hasValue(idProperty, accessor)) {
			return aggregate;
		}

		delegate.generateSequenceValue(idProperty, accessor);
		return accessor.getBean();
	}

}
