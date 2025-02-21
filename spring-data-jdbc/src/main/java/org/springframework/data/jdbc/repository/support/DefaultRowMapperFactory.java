package org.springframework.data.jdbc.repository.support;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.EntityRowMapper;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;

/**
 * Default implementation of {@link RowMapperFactory}. Honors the custom mappings defined
 * in {@link QueryMappingConfiguration}.
 * <p>
 * This implementation is not capable of loading the {@link RowMapper} or {@link ResultSetExtractor}
 * by reference via corresponding methods from {@link RowMapperFactory}.
 *
 * @implNote Public APIs of this class are thread-safe.
 * @author Mikhail Polivakha
 */
public class DefaultRowMapperFactory implements RowMapperFactory {

    private final RelationalMappingContext context;
    private final JdbcConverter converter;
    private final QueryMappingConfiguration queryMappingConfiguration;
    private final EntityCallbacks entityCallbacks;
    private final ApplicationEventPublisher publisher;

    public DefaultRowMapperFactory(
      RelationalMappingContext context,
      JdbcConverter converter,
      QueryMappingConfiguration queryMappingConfiguration,
      EntityCallbacks entityCallbacks,
      ApplicationEventPublisher publisher
    ) {
        this.context = context;
        this.converter = converter;
        this.queryMappingConfiguration = queryMappingConfiguration;
        this.entityCallbacks = entityCallbacks;
        this.publisher = publisher;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowMapper<Object> getRowMapper(Class<?> returnedObjectType) {

        RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(returnedObjectType);

        if (persistentEntity == null) {
            return (RowMapper<Object>) SingleColumnRowMapper.newInstance(returnedObjectType,
              converter.getConversionService());
        }

        return (RowMapper<Object>) determineDefaultMapper(returnedObjectType);
    }

    private RowMapper<?> determineDefaultMapper(Class<?> returnedObjectType) {

        RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(returnedObjectType);

        if (configuredQueryMapper != null) {
            return configuredQueryMapper;
        }

        EntityRowMapper<?> defaultEntityRowMapper = new EntityRowMapper<>( //
          context.getRequiredPersistentEntity(returnedObjectType), //
          converter //
        );

        return new CallbackAwareRowMapper<>(defaultEntityRowMapper, publisher, entityCallbacks);
    }
}
