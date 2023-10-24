/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query.template;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.DynamicTemplateBasedR2dbcQuery;
import org.springframework.data.relational.core.query.template.AbstractDynamicTemplateProvider;
import org.springframework.data.relational.core.query.template.TemplateStatement;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link DynamicTemplateBasedR2dbcQuery}.
 *
 * @author kfyty725
 */
@Configuration
@EnableR2dbcRepositories(value = "org.springframework.data.r2dbc.repository.query.template", considerNestedRepositories = true)
class DynamicTemplateQueryUnitTests {

    @Test
    void dynamicTemplate() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(DynamicTemplateQueryUnitTests.class);
        CarDao carDao = context.getBean(CarDao.class);

        List<Car> car = carDao.list(1L).collectList().block();          // id query, should one record
        List<Car> cars = carDao.list(null).collectList().block();       // null query, should two records

        Assertions.assertTrue(car.size() == 1);
        Assertions.assertTrue(cars.size() == 2);
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        return new H2ConnectionFactory(H2ConnectionConfiguration.builder()
                .inMemory("PUBLIC")
                .property("DB_CLOSE_DELAY", "-1")
                .username("SA")
                .password("12").build());
    }

    @Bean
    public R2dbcEntityTemplate r2dbcEntityTemplate(ConnectionFactory connectionFactory) {
        return new R2dbcEntityTemplate(connectionFactory);
    }

    @Bean
    public AbstractDynamicTemplateProvider<StringTemplateStatement> dynamicTemplateProvider() {
        AbstractDynamicTemplateProvider<StringTemplateStatement> templateProvider = new AbstractDynamicTemplateProvider<>() {

            @Override
            public String renderTemplate(String statementId, Map<String, Object> params) {
                Object[] param = (Object[]) params.get("root");
                StringTemplateStatement templateStatement = this.templateStatements.get(statementId);
                if (param[0] == null) {
                    return templateStatement.getTemplate().substring(0, templateStatement.getTemplate().lastIndexOf("#if root[0]"));
                }
                return templateStatement.getTemplate().replace("#if root[0]", "");
            }

            @Override
            protected StringTemplateStatement resolveInternal(String namespace, String id, String labelType, String content) {
                return new StringTemplateStatement(id, labelType, content);
            }
        };
        templateProvider.setTemplatePath(Collections.singletonList("/r2dbc/*.xml"));
        return templateProvider;
    }

    @Bean
    public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(connectionFactory);
        initializer.setDatabasePopulator(new ResourceDatabasePopulator(new ByteArrayResource("""
                CREATE TABLE car
                (
                    id BIGINT NOT NULL COMMENT '主键ID',
                    PRIMARY KEY (id)
                );
                insert into car values(1);
                insert into car values(2);
                """.getBytes(StandardCharsets.UTF_8))));
        return initializer;
    }

    @Repository
    interface CarDao extends R2dbcRepository<Car, Long> {
        @Query
        Flux<Car> list(Long id);
    }

    @Data
    static class Car {
        @Id
        private Long id;
    }

    @Getter
    @Setter
    static class StringTemplateStatement extends TemplateStatement {
        private String template;

        public StringTemplateStatement(String id, String labelType, String template) {
            super(id, labelType);
            this.template = template;
        }
    }
}
