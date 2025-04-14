/*
 * Copyright 2020-2025 the original author or authors.
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

package org.springframework.data.r2dbc.core.mapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.Assert;

import reactor.core.publisher.Mono;

/**
 * R2DBC Callback for generating ID via the database sequence.
 *
 * @author Mikhail Polivakha
 */
public class IdGeneratingBeforeSaveCallback implements BeforeSaveCallback<Object> {

	private static final Log LOG = LogFactory.getLog(IdGeneratingBeforeSaveCallback.class);

	private final RelationalMappingContext relationalMappingContext;
	private final R2dbcDialect dialect;

	private final DatabaseClient databaseClient;

	public IdGeneratingBeforeSaveCallback(RelationalMappingContext relationalMappingContext, R2dbcDialect dialect,
			DatabaseClient databaseClient) {
		this.relationalMappingContext = relationalMappingContext;
		this.dialect = dialect;
		this.databaseClient = databaseClient;
	}

	@Override
	public Publisher<Object> onBeforeSave(Object entity, OutboundRow row, SqlIdentifier table) {
		Assert.notNull(entity, "The aggregate cannot be null at this point");

		RelationalPersistentEntity<?> persistentEntity = relationalMappingContext.getPersistentEntity(entity.getClass());

		if (!persistentEntity.hasIdProperty() || //
				!persistentEntity.getIdProperty().hasSequence() || //
				!persistentEntity.isNew(entity) //
		) {
			return Mono.just(entity);
		}

		RelationalPersistentProperty property = persistentEntity.getIdProperty();
		SqlIdentifier idSequence = property.getSequence();

		if (dialect.getIdGeneration().sequencesSupported()) {
			return fetchIdFromSeq(entity, row, persistentEntity, idSequence);
		} else {
			illegalSequenceUsageWarning(entity);
		}

		return Mono.just(entity);
	}

	private Mono<Object> fetchIdFromSeq(Object entity, OutboundRow row, RelationalPersistentEntity<?> persistentEntity,
			SqlIdentifier idSequence) {
		String sequenceQuery = dialect.getIdGeneration().createSequenceQuery(idSequence);

		return databaseClient //
				.sql(sequenceQuery) //
				.map((r, rowMetadata) -> r.get(0)) //
				.one() //
				.map(fetchedId -> { //
					row.put( //
							persistentEntity.getIdColumn().toSql(dialect.getIdentifierProcessing()), //
							Parameter.from(fetchedId) //
					);
					return entity;
				});
	}

	private static void illegalSequenceUsageWarning(Object entity) {
		LOG.warn("""
				It seems you're trying to insert an aggregate of type '%s' annotated with @Sequence, but the problem is RDBMS you're
				working with does not support sequences as such. Falling back to identity columns
				""".stripIndent().formatted(entity.getClass().getName()));
	}
}
