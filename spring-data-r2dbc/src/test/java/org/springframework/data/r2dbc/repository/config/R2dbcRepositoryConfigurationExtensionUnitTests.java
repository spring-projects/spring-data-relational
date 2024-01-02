/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.config;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * Unit tests for {@link R2dbcRepositoryConfigurationExtension}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
class R2dbcRepositoryConfigurationExtensionUnitTests {

	private final AnnotationMetadata metadata = AnnotationMetadata.introspect(Config.class);
	private final ResourceLoader loader = new PathMatchingResourcePatternResolver();
	private final Environment environment = new StandardEnvironment();
	private final BeanDefinitionRegistry registry = new DefaultListableBeanFactory();

	private final RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(
			metadata, EnableR2dbcRepositories.class, loader, environment, registry);

	@Test // gh-13
	void isStrictMatchIfDomainTypeIsAnnotatedWithDocument() {

		R2dbcRepositoryConfigurationExtension extension = new R2dbcRepositoryConfigurationExtension();
		assertHasRepo(SampleRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // gh-13
	void isStrictMatchIfRepositoryExtendsStoreSpecificBase() {

		R2dbcRepositoryConfigurationExtension extension = new R2dbcRepositoryConfigurationExtension();
		assertHasRepo(StoreRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // gh-13
	void isNotStrictMatchIfDomainTypeIsNotAnnotatedWithDocument() {

		R2dbcRepositoryConfigurationExtension extension = new R2dbcRepositoryConfigurationExtension();
		assertDoesNotHaveRepo(UnannotatedRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // gh-13
	void doesNotHaveNonReactiveRepository() {

		R2dbcRepositoryConfigurationExtension extension = new R2dbcRepositoryConfigurationExtension();
		assertDoesNotHaveRepo(NonReactiveRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	private static void assertHasRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				return;
			}
		}

		fail("Expected to find config for repository interface ".concat(repositoryInterface.getName()).concat(" but got ")
				.concat(configs.toString()));
	}

	private static void assertDoesNotHaveRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				fail("Expected not to find config for repository interface ".concat(repositoryInterface.getName()));
			}
		}
	}

	@EnableR2dbcRepositories(considerNestedRepositories = true)
	private static class Config {}

	@Table("sample")
	static class Sample {}

	static class Store {}

	interface SampleRepository extends ReactiveCrudRepository<Sample, Long> {}

	interface UnannotatedRepository extends ReactiveCrudRepository<Store, Long> {}

	interface StoreRepository extends R2dbcRepository<Store, Long> {}

	interface NonReactiveRepository extends CrudRepository<Sample, Long> {}
}
