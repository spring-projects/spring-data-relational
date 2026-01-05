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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
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
	private final static MapSqlParameterSource EMPTY_PARAMETERS = new MapSqlParameterSource();

	private final Dialect dialect;
	private final NamedParameterJdbcOperations operations;

	public SequenceEntityCallbackDelegate(Dialect dialect, NamedParameterJdbcOperations operations) {
		this.dialect = dialect;
		this.operations = operations;
	}

	@SuppressWarnings("unchecked")
	protected void generateSequenceValue(RelationalPersistentProperty property,
			PersistentPropertyAccessor<Object> accessor) {

		Object sequenceValue = getSequenceValue(property);

		if (sequenceValue == null) {
			return;
		}

		Class<?> targetType = ClassUtils.resolvePrimitiveIfNecessary(property.getType());
		if (sequenceValue instanceof Number && Number.class.isAssignableFrom(targetType)) {
			sequenceValue = NumberUtils.convertNumberToTargetClass((Number) sequenceValue,
					(Class<? extends Number>) targetType);
		}

		accessor.setProperty(property, sequenceValue);
	}

	protected boolean hasValue(PersistentProperty<?> property, PersistentPropertyAccessor<Object> propertyAccessor) {

		Object identifier = propertyAccessor.getProperty(property);

		if (property.getType().isPrimitive()) {

			Object primitiveDefault = ReflectionUtils.getPrimitiveDefault(property.getType());
			return !primitiveDefault.equals(identifier);
		}

		return identifier != null;
	}

	private @Nullable Object getSequenceValue(RelationalPersistentProperty property) {

		SqlIdentifier sequence = property.getSequence();

		if (sequence == null) {
			return null;
		}

		if (!dialect.getIdGeneration().sequencesSupported()) {
			LOG.warn("""
					Aggregate type '%s' is marked for sequence usage but configured dialect '%s'
					does not support sequences. Falling back to identity columns.
					""".formatted(property.getOwner().getType(), ClassUtils.getQualifiedName(dialect.getClass())));
			return null;
		}

		String sql = dialect.getIdGeneration().createSequenceQuery(sequence);
		return operations.queryForObject(sql, EMPTY_PARAMETERS, (rs, rowNum) -> rs.getObject(1));
	}

}
