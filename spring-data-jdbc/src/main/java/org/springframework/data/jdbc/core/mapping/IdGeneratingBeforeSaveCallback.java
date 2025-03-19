package org.springframework.data.jdbc.core.mapping;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Callback for generating ID via the database sequence. By default, it is registered as a bean in
 * {@link AbstractJdbcConfiguration}
 *
 * @author Mikhail Polivakha
 */
public class IdGeneratingBeforeSaveCallback implements BeforeSaveCallback<Object> {

	private static final Log LOG = LogFactory.getLog(IdGeneratingBeforeSaveCallback.class);

	private final RelationalMappingContext relationalMappingContext;
	private final Dialect dialect;
	private final NamedParameterJdbcOperations operations;

	public IdGeneratingBeforeSaveCallback(RelationalMappingContext relationalMappingContext, Dialect dialect,
			NamedParameterJdbcOperations namedParameterJdbcOperations) {
		this.relationalMappingContext = relationalMappingContext;
		this.dialect = dialect;
		this.operations = namedParameterJdbcOperations;
	}

	@Override
	public Object onBeforeSave(Object aggregate, MutableAggregateChange<Object> aggregateChange) {

		Assert.notNull(aggregate, "The aggregate cannot be null at this point");

		RelationalPersistentEntity<?> persistentEntity = relationalMappingContext.getPersistentEntity(aggregate.getClass());

		if (!persistentEntity.hasIdProperty()) {
			return aggregate;
		}

		// we're doing INSERT and ID property value is not set explicitly by client
		if (persistentEntity.isNew(aggregate) && !hasIdentifierValue(aggregate, persistentEntity)) {
			return potentiallyFetchIdFromSequence(aggregate, persistentEntity);
		} else {
			return aggregate;
		}
	}

	private boolean hasIdentifierValue(Object aggregate, RelationalPersistentEntity<?> persistentEntity) {
		Object identifier = persistentEntity.getIdentifierAccessor(aggregate).getIdentifier();

		if (persistentEntity.getIdProperty().getType().isPrimitive()) {
			return identifier instanceof Number num && num.longValue() != 0L;
		} else {
			return identifier != null;
		}
	}

	private Object potentiallyFetchIdFromSequence(Object aggregate, RelationalPersistentEntity<?> persistentEntity) {
		Optional<SqlIdentifier> idSequence = persistentEntity.getIdSequence();

		if (dialect.getIdGeneration().sequencesSupported()) {
			idSequence.map(s -> dialect.getIdGeneration().createSequenceQuery(s)).ifPresent(sql -> {
				Long idValue = operations.queryForObject(sql, Map.of(), (rs, rowNum) -> rs.getLong(1));
				PersistentPropertyAccessor<Object> propertyAccessor = persistentEntity.getPropertyAccessor(aggregate);
				propertyAccessor.setProperty(persistentEntity.getRequiredIdProperty(), idValue);
			});
		} else {
			if (idSequence.isPresent()) {
				LOG.warn("""
						It seems you're trying to insert an aggregate of type '%s' annotated with @TargetSequence, but the problem is RDBMS you're
						working with does not support sequences as such. Falling back to identity columns
						""".formatted(aggregate.getClass().getName()));
			}
		}

		return aggregate;
	}
}
