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
package org.springframework.data.jdbc.repository.aot;

import java.lang.reflect.Method;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.jdbc.repository.support.BeanFactoryAwareRowMapperFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.repository.aot.generate.AotRepositoryClassBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * JDBC-specific {@link RepositoryContributor} contributing an AOT repository fragment.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class JdbcRepositoryContributor extends RepositoryContributor {

	private final RelationalMappingContext mappingContext;
	private final QueriesFactory queriesFactory;
	private final @Nullable String jdbcAggregateOperationsRef;

	public JdbcRepositoryContributor(AotRepositoryContext repositoryContext, JdbcDialect dialect,
			RelationalMappingContext mappingContext) {

		super(repositoryContext);

		this.mappingContext = mappingContext;

		RepositoryConfigurationSource configurationSource = repositoryContext.getConfigurationSource();

		this.queriesFactory = new QueriesFactory(configurationSource, dialect, mappingContext,
				repositoryContext.getRequiredClassLoader(), ValueExpressionDelegate.create());

		jdbcAggregateOperationsRef = configurationSource.getAttribute("jdbcAggregateOperationsRef").orElse(null);
	}

	@Override
	protected void customizeClass(AotRepositoryClassBuilder classBuilder) {
		classBuilder.customize(builder -> builder.superclass(TypeName.get(AotRepositoryFragmentSupport.class)));
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {

		constructorBuilder.addParameter("operations", JdbcAggregateOperations.class, customizer -> {

			customizer.origin(StringUtils.hasText(jdbcAggregateOperationsRef)
					? new RuntimeBeanReference(jdbcAggregateOperationsRef, JdbcAggregateOperations.class)
					: new RuntimeBeanReference(JdbcAggregateOperations.class));
		});

		constructorBuilder.addParameter("rowMapperFactory", RowMapperFactory.class, customizer -> {

			customizer.origin(ctx -> {

				String rowMapperFactory = ctx.localVariable("rowMapperFactory");
				String operations = ctx.localVariable("operations");
				CodeBlock.Builder builder = CodeBlock.builder();
				builder.addStatement("$1T $2L = new $1T($4L, $3L, $4L.getBeanProvider($5T.class).getIfUnique(() -> $5T.EMPTY))",
						BeanFactoryAwareRowMapperFactory.class, rowMapperFactory, operations, ctx.beanFactory(),
						QueryMappingConfiguration.class);

				return AotRepositoryConstructorBuilder.ParameterOrigin.of(rowMapperFactory, builder.build());
			});
		});

		constructorBuilder.addParameter("context", RepositoryFactoryBeanSupport.FragmentCreationContext.class, false);
	}

	@Override
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method) {

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, getRepositoryInformation(), getProjectionFactory(),
				queriesFactory.getNamedQueries(), mappingContext);

		ReturnedType returnedType = queryMethod.getResultProcessor().getReturnedType();
		MergedAnnotation<Query> query = MergedAnnotations.from(method).get(Query.class);
		AotQueries aotQueries = queriesFactory.createQueries(getRepositoryInformation(), returnedType, query, queryMethod);

		if (queryMethod.isModifyingQuery()) {

			TypeInformation<?> returnType = getRepositoryInformation().getReturnType(method);

			boolean returnsCount = JdbcCodeBlocks.QueryExecutionBlockBuilder.returnsModifying(returnType.getType());
			boolean isVoid = ClassUtils.isVoidType(returnType.getType());

			if (!returnsCount && !isVoid) {
				return MethodContributor.forQueryMethod(queryMethod).metadataOnly(aotQueries.toMetadata());
			}
		}

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(aotQueries.toMetadata()).contribute(context -> {

			CodeBlock.Builder body = CodeBlock.builder();

			MergedAnnotation<Modifying> modifying = context.getAnnotation(Modifying.class);
			MergedAnnotation<Lock> lock = context.getAnnotation(Lock.class);

			String queryVariable = context.localVariable("query");
			String parameterSourceVariable = context.localVariable("parameterSource");

			body.add(JdbcCodeBlocks.queryBuilder(context, queryMethod).filter(aotQueries)
					.usingQueryVariableName(queryVariable).parameterSource(parameterSourceVariable).lock(lock).build());

			body.add("\n");

			body.add(JdbcCodeBlocks.executionBuilder(context, queryMethod).modifying(modifying)
					.usingQueryVariableName(queryVariable).parameterSource(parameterSourceVariable).queries(aotQueries)
					.queryAnnotation(query).build());

			return body.build();
		});

	}
}
