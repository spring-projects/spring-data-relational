/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.config.DefaultRepositoryBaseClass;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * Annotation to enable JDBC repositories. Will scan the package of the annotated configuration class for Spring Data
 * repositories by default.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Mark Paluch
 * @author Fei Dong
 * @author Antoine Sauray
 * @author Diego Krupitza
 * @author Christoph Strobl
 * @see AbstractJdbcConfiguration
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@Import(JdbcRepositoriesRegistrar.class)
public @interface EnableJdbcRepositories {

	/**
	 * Alias for the {@link #basePackages()} attribute. Allows for more concise annotation declarations e.g.:
	 * {@code @EnableJdbcRepositories("org.my.pkg")} instead of
	 * {@code @EnableJdbcRepositories(basePackages="org.my.pkg")}.
	 */
	String[] value() default {};

	/**
	 * Base packages to scan for annotated components. {@link #value()} is an alias for (and mutually exclusive with) this
	 * attribute. Use {@link #basePackageClasses()} for a type-safe alternative to String-based package names.
	 */
	String[] basePackages() default {};

	/**
	 * Type-safe alternative to {@link #basePackages()} for specifying the packages to scan for annotated components. The
	 * package of each class specified will be scanned. Consider creating a special no-op marker class or interface in
	 * each package that serves no purpose other than being referenced by this attribute.
	 */
	Class<?>[] basePackageClasses() default {};

	/**
	 * Specifies which types are eligible for component scanning. Further narrows the set of candidate components from
	 * everything in {@link #basePackages()} to everything in the base packages that matches the given filter or filters.
	 */
	Filter[] includeFilters() default {};

	/**
	 * Specifies which types are not eligible for component scanning.
	 */
	Filter[] excludeFilters() default {};

	/**
	 * Returns the postfix to be used when looking up custom repository implementations. Defaults to {@literal Impl}. So
	 * for a repository named {@code PersonRepository} the corresponding implementation class will be looked up scanning
	 * for {@code PersonRepositoryImpl}.
	 */
	String repositoryImplementationPostfix() default "Impl";

	/**
	 * Configures the location of where to find the Spring Data named queries properties file. Will default to
	 * {@code META-INF/jdbc-named-queries.properties}.
	 */
	String namedQueriesLocation() default "";

	/**
	 * Returns the {@link FactoryBean} class to be used for each repository instance. Defaults to
	 * {@link JdbcRepositoryFactoryBean}.
	 */
	Class<?> repositoryFactoryBeanClass() default JdbcRepositoryFactoryBean.class;

	/**
	 * Configure the repository base class to be used to create repository proxies for this particular configuration.
	 *
	 * @since 2.1
	 */
	Class<?> repositoryBaseClass() default DefaultRepositoryBaseClass.class;

	/**
	 * Configure a specific {@link BeanNameGenerator} to be used when creating the repository beans.
	 * @return the {@link BeanNameGenerator} to be used or the base {@link BeanNameGenerator} interface to indicate context default.
	 * @since 3.4
	 */
	Class<? extends BeanNameGenerator> nameGenerator() default BeanNameGenerator.class;

	/**
	 * Configures whether nested repository-interfaces (e.g. defined as inner classes) should be discovered by the
	 * repositories infrastructure.
	 */
	boolean considerNestedRepositories() default false;

	/**
	 * Configures the name of the {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations} bean
	 * definition to be used to create repositories discovered through this annotation. Defaults to
	 * {@code namedParameterJdbcTemplate}.
	 */
	String jdbcOperationsRef() default "";

	/**
	 * Configures the name of the {@link org.springframework.data.jdbc.core.convert.DataAccessStrategy} bean definition to
	 * be used to create repositories discovered through this annotation. Defaults to {@code defaultDataAccessStrategy}.
	 */
	String dataAccessStrategyRef() default "";

	/**
	 * Configures the name of the {@link DataSourceTransactionManager} bean definition to be used to create repositories
	 * discovered through this annotation. Defaults to {@code transactionManager}.
	 * 
	 * @since 2.1
	 */
	String transactionManagerRef() default "transactionManager";

	/**
	 * Returns the key of the {@link QueryLookupStrategy} to be used for lookup queries for query methods. Defaults to
	 * {@link QueryLookupStrategy.Key#CREATE_IF_NOT_FOUND}.
	 *
	 * @since 2.1
	 */
	QueryLookupStrategy.Key queryLookupStrategy() default QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND;
}
