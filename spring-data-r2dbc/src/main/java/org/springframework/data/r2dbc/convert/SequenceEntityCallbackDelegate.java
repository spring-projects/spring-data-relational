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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;

/**
 * Support class for generating identifier values through a database sequence.
 *
 * @author Mikhail Polivakha
 * @author Mark Paluch
 * @since 3.5
 * @see org.springframework.data.relational.core.mapping.Sequence
 */
class SequenceEntityCallbackDelegate {

	private static final Log LOG = LogFactory.getLog(SequenceEntityCallbackDelegate.class);

	private final Dialect dialect;
	private final DatabaseClient databaseClient;

	public SequenceEntityCallbackDelegate(Dialect dialect, DatabaseClient databaseClient) {
		this.dialect = dialect;
		this.databaseClient = databaseClient;
	}

	@SuppressWarnings("unchecked")
	protected Mono<Object> generateSequenceValue(RelationalPersistentProperty property, OutboundRow row,
			PersistentPropertyAccessor<Object> accessor) {

		Class<?> targetType = ClassUtils.resolvePrimitiveIfNecessary(property.getType());

		return getSequenceValue(property).map(it -> {

			Object sequenceValue = it;
			if (sequenceValue instanceof Number && Number.class.isAssignableFrom(targetType)) {
				sequenceValue = NumberUtils.convertNumberToTargetClass((Number) sequenceValue,
						(Class<? extends Number>) targetType);
			}

			row.append(property.getColumnName(), Parameter.from(sequenceValue));
			accessor.setProperty(property, sequenceValue);

			return accessor.getBean();
		});
	}

	protected boolean hasValue(PersistentProperty<?> property, PersistentPropertyAccessor<Object> propertyAccessor) {

		Object identifier = propertyAccessor.getProperty(property);

		if (property.getType().isPrimitive()) {

			Object primitiveDefault = ReflectionUtils.getPrimitiveDefault(property.getType());
			return !primitiveDefault.equals(identifier);
		}

		return identifier != null;
	}

	private Mono<Object> getSequenceValue(RelationalPersistentProperty property) {

		SqlIdentifier sequence = property.getSequence();

		if (sequence == null) {
			return Mono.empty();
		}

		if (!dialect.getIdGeneration().sequencesSupported()) {
			LOG.warn("""
					Entity type '%s' is marked for sequence usage but configured dialect '%s'
					does not support sequences. Falling back to identity columns.
					""".formatted(property.getOwner().getType(), ClassUtils.getQualifiedName(dialect.getClass())));
			return Mono.empty();
		}

		String sql = dialect.getIdGeneration().createSequenceQuery(sequence);
		return databaseClient //
				.sql(sql) //
				.map((r, rowMetadata) -> r.get(0)) //
				.one();
	}

}
