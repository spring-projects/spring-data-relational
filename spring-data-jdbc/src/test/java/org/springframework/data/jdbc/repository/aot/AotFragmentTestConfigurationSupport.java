/*
 * Copyright 2025 the original author or authors.
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
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.mockito.Mockito;

import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultBeanNameGenerator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.jdbc.repository.support.BeanFactoryAwareRowMapperFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFragment;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.ReflectionUtils;

/**
 * Test Configuration Support Class for generated AOT Repository Fragments based on a Repository Interface.
 * <p>
 * This configuration generates the AOT repository, compiles sources and configures a BeanFactory to contain the AOT
 * fragment. Additionally, the fragment is exposed through a {@code repositoryInterface} JDK proxy forwarding method
 * invocations to the backing AOT fragment. Note that {@code repositoryInterface} is not a repository proxy.
 *
 * @author Mark Paluch
 */
public class AotFragmentTestConfigurationSupport implements BeanFactoryPostProcessor, ApplicationContextAware {

	private final Class<?> repositoryInterface;
	private final JdbcDialect dialect;
	private final boolean registerFragmentFacade;
	private final TestJdbcAotRepositoryContext<?> repositoryContext;
	private ApplicationContext applicationContext;

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface, JdbcDialect dialect, Class<?> configClass) {
		this(repositoryInterface, dialect, configClass, true);
	}

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface, JdbcDialect dialect, Class<?> configClass,
			boolean registerFragmentFacade, Class<?>... additionalFragments) {

		this.repositoryInterface = repositoryInterface;
		this.dialect = dialect;

		RepositoryComposition composition = RepositoryComposition
				.of((List) Arrays.stream(additionalFragments).map(RepositoryFragment::structural).toList());
		this.repositoryContext = new TestJdbcAotRepositoryContext<>(repositoryInterface, composition,
				new AnnotationRepositoryConfigurationSource(AnnotationMetadata.introspect(configClass),
						EnableJdbcRepositories.class, new DefaultResourceLoader(), new StandardEnvironment(),
						Mockito.mock(BeanDefinitionRegistry.class), DefaultBeanNameGenerator.INSTANCE));
		this.registerFragmentFacade = registerFragmentFacade;
	}

	@Bean
	BeanFactoryAwareRowMapperFactory rowMapperFactory(ApplicationContext context,
			JdbcAggregateOperations aggregateOperations, Optional<QueryMappingConfiguration> queryMappingConfiguration) {
		return new BeanFactoryAwareRowMapperFactory(context, aggregateOperations,
				queryMappingConfiguration.orElse(QueryMappingConfiguration.EMPTY));
	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

		TestGenerationContext generationContext = new TestGenerationContext(repositoryInterface);

		repositoryContext.setBeanFactory(beanFactory);

		JdbcRepositoryContributor jdbcRepositoryContributor = new JdbcRepositoryContributor(repositoryContext, dialect,
				new MappingJdbcConverter(new JdbcMappingContext(), (identifier, path) -> null));
		jdbcRepositoryContributor.contribute(generationContext);

		AbstractBeanDefinition aotGeneratedRepository = BeanDefinitionBuilder
				.genericBeanDefinition(repositoryInterface.getName() + "Impl__AotRepository")
				.addConstructorArgValue(new RuntimeBeanReference(JdbcAggregateOperations.class))
				.addConstructorArgValue(new RuntimeBeanReference(RowMapperFactory.class))
				.addConstructorArgValue(
						getCreationContext(repositoryContext, beanFactory.getBean(Environment.class), beanFactory))
				.getBeanDefinition();

		generationContext.writeGeneratedContent();

		TestCompiler.forSystem().withCompilerOptions("-parameters").with(generationContext).compile(compiled -> {
			beanFactory.setBeanClassLoader(compiled.getClassLoader());
			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragment", aotGeneratedRepository);
		});

		if (registerFragmentFacade) {

			BeanDefinition fragmentFacade = BeanDefinitionBuilder.rootBeanDefinition((Class) repositoryInterface, () -> {

				Object fragment = beanFactory.getBean("fragment");
				Object proxy = getFragmentFacadeProxy(fragment);

				return repositoryInterface.cast(proxy);
			}).getBeanDefinition();
			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragmentFacade", fragmentFacade);
		}
	}

	private Object getFragmentFacadeProxy(Object fragment) {

		return Proxy.newProxyInstance(repositoryInterface.getClassLoader(), new Class<?>[] { repositoryInterface },
				(p, method, args) -> {

					Method target = ReflectionUtils.findMethod(fragment.getClass(), method.getName(), method.getParameterTypes());

					if (target == null) {
						throw new NoSuchMethodException("Method [%s] is not implemented by [%s]".formatted(method, target));
					}

					try {
						return target.invoke(fragment, args);
					} catch (ReflectiveOperationException e) {
						ReflectionUtils.handleReflectionException(e);
					}

					return null;
				});
	}

	private RepositoryFactoryBeanSupport.FragmentCreationContext getCreationContext(
			TestJdbcAotRepositoryContext<?> repositoryContext, Environment environment, ListableBeanFactory beanFactory) {

		RepositoryFactoryBeanSupport.FragmentCreationContext creationContext = new RepositoryFactoryBeanSupport.FragmentCreationContext() {
			@Override
			public RepositoryMetadata getRepositoryMetadata() {
				return repositoryContext.getRepositoryInformation();
			}

			@Override
			public ValueExpressionDelegate getValueExpressionDelegate() {

				QueryMethodValueEvaluationContextAccessor accessor = new QueryMethodValueEvaluationContextAccessor(environment,
						beanFactory);
				return new ValueExpressionDelegate(accessor, ValueExpressionParser.create());
			}

			@Override
			public ProjectionFactory getProjectionFactory() {
				return new SpelAwareProxyProjectionFactory();
			}
		};

		return creationContext;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
