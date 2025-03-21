package org.springframework.data.jdbc.core.mapping;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import static org.springframework.util.Assert.*;

/**
 * Callback for generating ID via the database sequence. By default, it is registered as a bean in
 * {@link AbstractJdbcConfiguration}
 *
 * @author Mikhail Polivakha
 */
public class IdGeneratingBeforeSaveCallback implements BeforeSaveCallback<Object> {

	private static final Log LOG = LogFactory.getLog(IdGeneratingBeforeSaveCallback.class);

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> relationalMappingContext;
	private final Dialect dialect;
	private final NamedParameterJdbcOperations operations;
	private final RelationalConverter converter;

	public IdGeneratingBeforeSaveCallback(Dialect dialect, NamedParameterJdbcOperations namedParameterJdbcOperations,
			RelationalConverter converter) {

		this.relationalMappingContext = converter.getMappingContext();
		this.dialect = dialect;
		this.operations = namedParameterJdbcOperations;
		this.converter = converter;
	}

	@Override
	public Object onBeforeSave(Object aggregate, MutableAggregateChange<Object> aggregateChange) {

		Assert.notNull(aggregate, "The aggregate must not be null at this point");

		RelationalPersistentEntity<?> persistentEntity = relationalMappingContext
				.getRequiredPersistentEntity(aggregate.getClass());
		Optional<SqlIdentifier> idSequence = persistentEntity.getIdSequence();

		if (!dialect.getIdGeneration().sequencesSupported()) {
			if (idSequence.isPresent()) {
				LOG.warn(
						"""
								It seems you're trying to insert an aggregate of type '%s' annotated with @TargetSequence, but the problem is RDBMS you're
								working with does not support sequences as such. Falling back to identity columns
								"""
								.formatted(aggregate.getClass().getName()));
			}
			return aggregate;
		}

		if (!persistentEntity.hasIdProperty()) {
			return aggregate;
		}

		PersistentPropertyAccessor<Object> accessor = persistentEntity.getPropertyAccessor(aggregate);

		idSequence.map(this::querySequence).ifPresent(idValue -> {
			RelationalPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();
			if (idProperty.isEmbedded()) {

				setEmbeddedIdValue(persistentEntity, idProperty, aggregate, idValue, accessor);

			} else {
				accessor.setProperty(idProperty, idValue);
			}

		});
		return accessor.getBean();
	}

	private void setEmbeddedIdValue(RelationalPersistentEntity<?> persistentEntity,
			RelationalPersistentProperty idProperty, Object aggregate, Long idValue,
			PersistentPropertyAccessor<Object> accessor) {

		Class<?> idPropertyType = idProperty.getType();
		RelationalPersistentEntity<?> idEntity = relationalMappingContext.getRequiredPersistentEntity(idPropertyType);

		RelationalPersistentProperty[] propertyHolder = new RelationalPersistentProperty[1];
		idEntity.doWithProperties((RelationalPersistentProperty property) -> {
			if (propertyHolder[0] != null) {
				throw new IllegalStateException(
						"There is no unique id property path for %s".formatted(persistentEntity.toString()));
			}
			propertyHolder[0] = property;
		});

		Object propertyValue = accessor.getProperty(propertyHolder[0]);

		if (propertyValue != null) {
			persistentEntity.getPropertyPathAccessor(aggregate).setProperty(propertyHolder[0], idValue);
		} else {
			Object idInstance = converter.getEntityInstantiators().getInstantiatorFor(idEntity).createInstance(idEntity,
					new ParameterValueProvider<RelationalPersistentProperty>() {
						@Nullable
						@Override
						public <T> T getParameterValue(Parameter<T, RelationalPersistentProperty> parameter) {
							return (T) idValue;
						}
					});
			accessor.setProperty(idProperty, idInstance);
		}
	}

	private Long querySequence(SqlIdentifier s) {

		String sql = dialect.getIdGeneration().createSequenceQuery(s);
		Long sequenceValue = operations.queryForObject(sql, Map.of(), Long.class);
		state(sequenceValue != null, () -> "No sequence value found for " + s);
		return sequenceValue;
	}
}
