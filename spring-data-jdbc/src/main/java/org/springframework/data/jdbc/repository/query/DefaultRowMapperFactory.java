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
package org.springframework.data.jdbc.repository.query;

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
    public RowMapper<Object> create(Class<?> returnedObjectType) {

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

        return new CallbackCapableRowMapper<>(defaultEntityRowMapper, publisher, entityCallbacks);
    }
}
