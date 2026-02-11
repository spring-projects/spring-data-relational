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
package org.springframework.data.r2dbc.convert;

import reactor.core.publisher.Mono;

import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.DatabaseClient;
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
			MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			R2dbcDialect dialect,
			DatabaseClient databaseClient) {

		this.context = context;
		this.delegate = new SequenceEntityCallbackDelegate(dialect, databaseClient);
	}

	@Override
	public Mono<Object> onBeforeSave(Object entity, OutboundRow row, SqlIdentifier table) {

		Assert.notNull(entity, "Entity must not be null");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(entity.getClass());

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();
		if (idProperty == null || !idProperty.hasSequence()) {
			return Mono.just(entity);
		}

		PersistentPropertyAccessor<Object> accessor = persistentEntity.getPropertyAccessor(entity);

		if (delegate.hasValue(idProperty, accessor)) {
			return Mono.just(entity);
		}

		Mono<Object> idGenerator = delegate.generateSequenceValue(idProperty, row, accessor);
		return idGenerator.defaultIfEmpty(entity);
	}

}
