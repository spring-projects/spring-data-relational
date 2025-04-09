package org.springframework.data.jdbc.core.mapping;

import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;

/**
 * Callback for generating identifier values through a database sequence.
 *
 * @author Mikhail Polivakha
 * @author Mark Paluch
 * @since 3.5
 * @see org.springframework.data.relational.core.mapping.Sequence
 */
public class IdGeneratingBeforeSaveCallback implements BeforeSaveCallback<Object> {

	private static final Log LOG = LogFactory.getLog(IdGeneratingBeforeSaveCallback.class);
	private final static MapSqlParameterSource EMPTY_PARAMETERS = new MapSqlParameterSource();

	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final Dialect dialect;
	private final NamedParameterJdbcOperations operations;

	public IdGeneratingBeforeSaveCallback(
			MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext,
			Dialect dialect, NamedParameterJdbcOperations operations) {
		this.mappingContext = mappingContext;
		this.dialect = dialect;
		this.operations = operations;
	}

	@Override
	public Object onBeforeSave(Object aggregate, MutableAggregateChange<Object> aggregateChange) {

		Assert.notNull(aggregate, "aggregate must not be null");

		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(aggregate.getClass());

		if (!entity.hasIdProperty()) {
			return aggregate;
		}

		RelationalPersistentProperty idProperty = entity.getRequiredIdProperty();
		PersistentPropertyAccessor<Object> accessor = entity.getPropertyAccessor(aggregate);

		if (!entity.isNew(aggregate) || hasIdentifierValue(idProperty, accessor)) {
			return aggregate;
		}

		potentiallyFetchIdFromSequence(idProperty, entity, accessor);
		return accessor.getBean();
	}

	private boolean hasIdentifierValue(PersistentProperty<?> idProperty,
			PersistentPropertyAccessor<Object> propertyAccessor) {

		Object identifier = propertyAccessor.getProperty(idProperty);

		if (idProperty.getType().isPrimitive()) {

			Object primitiveDefault = ReflectionUtils.getPrimitiveDefault(idProperty.getType());
			return !primitiveDefault.equals(identifier);
		}

		return identifier != null;
	}

	@SuppressWarnings("unchecked")
	private void potentiallyFetchIdFromSequence(PersistentProperty<?> idProperty,
			RelationalPersistentEntity<?> persistentEntity, PersistentPropertyAccessor<Object> accessor) {

		Optional<SqlIdentifier> idSequence = persistentEntity.getIdSequence();

		if (idSequence.isPresent() && !dialect.getIdGeneration().sequencesSupported()) {
			LOG.warn("""
					Aggregate type '%s' is marked for sequence usage but configured dialect '%s'
					does not support sequences. Falling back to identity columns.
					""".formatted(persistentEntity.getType(), ClassUtils.getQualifiedName(dialect.getClass())));
			return;
		}

		idSequence.map(s -> dialect.getIdGeneration().createSequenceQuery(s)).ifPresent(sql -> {

			Object idValue = operations.queryForObject(sql, EMPTY_PARAMETERS, (rs, rowNum) -> rs.getObject(1));

			Class<?> targetType = ClassUtils.resolvePrimitiveIfNecessary(idProperty.getType());
			if (idValue instanceof Number && Number.class.isAssignableFrom(targetType)) {
				accessor.setProperty(idProperty,
						NumberUtils.convertNumberToTargetClass((Number) idValue, (Class<? extends Number>) targetType));
			} else {
				accessor.setProperty(idProperty, idValue);
			}
		});
	}
}
