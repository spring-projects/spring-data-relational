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
package org.springframework.data.jdbc.repository.support;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.query.DefaultRowMapperFactory;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * This {@link RowMapperFactory} implementation extends the {@link DefaultRowMapperFactory}
 * by adding the capabilities to load {@link RowMapper} or {@link ResultSetExtractor} beans by
 * their names in {@link BeanFactory}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mikhail Polivakha
 */
@SuppressWarnings("unchecked")
public class BeanFactoryAwareRowMapperFactory extends DefaultRowMapperFactory {

    private final @Nullable BeanFactory beanFactory;

    public BeanFactoryAwareRowMapperFactory(
      RelationalMappingContext context,
      JdbcConverter converter,
      QueryMappingConfiguration queryMappingConfiguration,
      EntityCallbacks entityCallbacks,
      ApplicationEventPublisher publisher,
      @Nullable BeanFactory beanFactory
    ) {
        super(context, converter, queryMappingConfiguration, entityCallbacks, publisher);

        this.beanFactory = beanFactory;
    }

    @Override
    public RowMapper<Object> getRowMapper(String reference) {
        if (beanFactory == null) {
            throw new IllegalStateException(
              "Cannot resolve RowMapper bean reference '" + reference + "'; BeanFactory is not configured.");
        }

        return beanFactory.getBean(reference, RowMapper.class);
    }

    @Override
    public ResultSetExtractor<Object> getResultSetExtractor(String reference) {
        if (beanFactory == null) {
            throw new IllegalStateException(
              "Cannot resolve ResultSetExtractor bean reference '" + reference + "'; BeanFactory is not configured.");
        }

        return beanFactory.getBean(reference, ResultSetExtractor.class);
    }
}
