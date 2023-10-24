/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.query.template.DynamicTemplateProvider;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.SpelQueryContext;
import org.springframework.data.util.Pair;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Dynamic template-based {@link DynamicTemplateBasedR2dbcQuery} implementation.
 * Dynamic template query, using external templates to provide dynamic SQL support, example: enjoy, freemarker, st etc...
 *
 * @author kfyty725
 * @email kfyty725@hotmail.com
 */
public class DynamicTemplateBasedR2dbcQuery extends StringBasedR2dbcQuery {
    private static final Log LOG = LogFactory.getLog(DynamicTemplateBasedR2dbcQuery.class);

    /**
     * Is it executing? Initialize the instance to false and create a new execution every time the statement is executed
     */
    private final boolean isRuntime;

    /**
     * Interface method
     */
    private final Method method;

    /**
     * The currently executing query statement
     */
    private final String query;

    /**
     * {@link DynamicTemplateProvider}
     */
    private final DynamicTemplateProvider<?> dynamicTemplateProvider;

    /**
     * {@link R2dbcQueryMethod} producer
     */
    private final Supplier<R2dbcQueryMethod> queryMethodProducer;

    public DynamicTemplateBasedR2dbcQuery(boolean isRuntime,
                                          Method method,
                                          String query,
                                          DynamicTemplateProvider<?> dynamicTemplateProvider,
                                          Supplier<R2dbcQueryMethod> queryMethodProducer,
                                          R2dbcQueryMethod queryMethod,
                                          R2dbcEntityOperations entityOperations,
                                          R2dbcConverter converter,
                                          ReactiveDataAccessStrategy dataAccessStrategy,
                                          ExpressionParser expressionParser,
                                          ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {
        super(query, queryMethod, entityOperations, converter, dataAccessStrategy, expressionParser, evaluationContextProvider);
        this.isRuntime = isRuntime;
        this.method = method;
        this.query = query;
        this.dynamicTemplateProvider = dynamicTemplateProvider;
        this.queryMethodProducer = queryMethodProducer;
    }

    @Override
    public Object execute(Object[] parameters) {
        if (this.dynamicTemplateProvider == null || !this.isDynamicTemplateQuery()) {
            return super.execute(parameters);
        }

        if (!this.isRuntime) {
            // When querying, it is necessary to first remove unused parameters, otherwise parameter binding will fail
            String query = this.resolveTemplateQuery(parameters);
            Pair<R2dbcQueryMethod, List<RelationalParameters.RelationalParameter>> relationalParameters = obtainNewMethodAndRelationalParameters();
            List<String> used = searchQueryParameters(query);
            this.removeUnusedParameter(used, relationalParameters.getSecond());
            return new DynamicTemplateBasedR2dbcQuery(
                    true,
                    method,
                    query,
                    dynamicTemplateProvider,
                    queryMethodProducer,
                    relationalParameters.getFirst(),
                    entityOperations,
                    converter,
                    dataAccessStrategy,
                    expressionParser,
                    evaluationContextProvider).execute(parameters);
        }

        return super.execute(parameters);
    }

    /**
     * Is it a dynamic template query
     *
     * @return true if dynamic template
     */
    protected boolean isDynamicTemplateQuery() {
        return super.method.getRequiredAnnotatedQuery().isBlank();
    }

    /**
     * Obtain dynamic template SQL
     *
     * @param parameters Method parameters
     * @return SQL
     */
    protected String resolveTemplateQuery(Object[] parameters) {
        if (!this.isDynamicTemplateQuery()) {
            return this.query;
        }
        Assert.notNull(this.dynamicTemplateProvider, "dynamicTemplateProvider can't null");
        String statementId = this.dynamicTemplateProvider.resolveTemplateStatementId(this.method);
        String query = this.dynamicTemplateProvider.renderTemplate(statementId, Map.of("root", parameters));
        LOG.debug("resolve template SQL: " + query);
        return query;
    }

    /**
     * Search SQL Query Parameters
     *
     * @param query SQL
     * @return Parameters and Index
     */
    public static List<String> searchQueryParameters(String query) {
        List<String> parameterBindings = new LinkedList<>();
        SpelQueryContext queryContext = SpelQueryContext.of((counter, expression) -> {
            parameterBindings.add(expression);
            return expression;
        }, String::concat);
        SpelQueryContext.SpelExtractor parsed = queryContext.parse(query);
        return parameterBindings;
    }

    /**
     * Build a new {@link R2dbcQueryMethod} and obtain the relational parameter
     */
    @SuppressWarnings("unchecked")
    private Pair<R2dbcQueryMethod, List<RelationalParameters.RelationalParameter>> obtainNewMethodAndRelationalParameters() {
        R2dbcQueryMethod r2dbcQueryMethod = this.queryMethodProducer.get();
        RelationalParameters parameters = r2dbcQueryMethod.getParameters().getBindableParameters();
        Field field = ReflectionUtils.findField(parameters.getClass(), "parameters");
        ReflectionUtils.makeAccessible(field);
        List<RelationalParameters.RelationalParameter> list = (List<RelationalParameters.RelationalParameter>) ReflectionUtils.getField(field, parameters);
        return Pair.of(r2dbcQueryMethod, list);
    }

    /**
     * Remove unused parameters
     *
     * @param used Parameters used in SQL
     * @param list Original parameters
     */
    private void removeUnusedParameter(List<String> used, List<RelationalParameters.RelationalParameter> list) {
        loop:
        for (Iterator<RelationalParameters.RelationalParameter> i = list.iterator(); i.hasNext(); ) {
            String name = i.next().getName().get();
            for (String usedName : used) {
                if (usedName.contains(name)) {
                    continue loop;
                }
            }
            i.remove();
        }
    }
}
