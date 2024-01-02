/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Unit tests for {@link R2dbcAuditingRegistrar}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class R2dbcAuditingRegistrarUnitTests {

	private final R2dbcAuditingRegistrar registrar = new R2dbcAuditingRegistrar();

	@Mock AnnotationMetadata metadata;
	@Mock BeanDefinitionRegistry registry;

	@Test // gh-281
	void rejectsNullAnnotationMetadata() {
		assertThatIllegalArgumentException().isThrownBy(() -> registrar.registerBeanDefinitions(null, registry));
	}

	@Test // gh-281
	void rejectsNullBeanDefinitionRegistry() {
		assertThatIllegalArgumentException().isThrownBy(() -> registrar.registerBeanDefinitions(metadata, null));
	}
}
