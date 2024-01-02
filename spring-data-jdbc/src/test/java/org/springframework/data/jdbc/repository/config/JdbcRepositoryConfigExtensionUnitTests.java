/*
 * Copyright 2019-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;

/**
 * Unit tests for {@link JdbcRepositoryConfigExtension}.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryConfigExtensionUnitTests {

	AnnotationMetadata metadata = AnnotationMetadata.introspect(Config.class);
	ResourceLoader loader = new PathMatchingResourcePatternResolver();
	Environment environment = new StandardEnvironment();
	BeanDefinitionRegistry registry = new DefaultListableBeanFactory();

	RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
			EnableJdbcRepositories.class, loader, environment, registry, null);

	@Test // DATAJPA-437
	public void isStrictMatchOnlyIfDomainTypeIsAnnotatedWithDocument() {

		JdbcRepositoryConfigExtension extension = new JdbcRepositoryConfigExtension();

		Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs = extension
				.getRepositoryConfigurations(configurationSource, loader, true);

		assertThat(configs).extracting(config -> config.getRepositoryInterface())
				.containsExactly(SampleRepository.class.getName());
	}

	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

	}

	@Table
	static class Sample {}

	interface SampleRepository extends Repository<Sample, Long> {}

	static class Unannotated {}

	interface UnannotatedRepository extends Repository<Unannotated, Long> {}
}
