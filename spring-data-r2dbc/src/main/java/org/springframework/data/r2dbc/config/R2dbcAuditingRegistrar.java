/*
 * Copyright 2020-2022 the original author or authors.
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

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.r2dbc.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ImportBeanDefinitionRegistrar} to enable {@link EnableR2dbcAuditing} annotation.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.2
 */
class R2dbcAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAnnotation()
	 */
	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableR2dbcAuditing.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditingHandlerBeanName()
	 */
	@Override
	protected String getAuditingHandlerBeanName() {
		return "r2dbcAuditingHandler";
	}

	@Override
	protected void postProcess(BeanDefinitionBuilder builder, AuditingConfiguration configuration,
			BeanDefinitionRegistry registry) {
		potentiallyRegisterR2dbcPersistentEntities(builder, registry);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditHandlerBeanDefinitionBuilder(org.springframework.data.auditing.config.AuditingConfiguration)
	 */
	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {

		Assert.notNull(configuration, "AuditingConfiguration must not be null");

		return configureDefaultAuditHandlerAttributes(configuration,
				BeanDefinitionBuilder.rootBeanDefinition(ReactiveIsNewAwareAuditingHandler.class));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#registerAuditListener(org.springframework.beans.factory.config.BeanDefinition, org.springframework.beans.factory.support.BeanDefinitionRegistry)
	 */
	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {

		Assert.notNull(auditingHandlerDefinition, "BeanDefinition must not be null");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null");

		BeanDefinitionBuilder listenerBeanDefinitionBuilder = BeanDefinitionBuilder
				.rootBeanDefinition(ReactiveAuditingEntityCallback.class);
		listenerBeanDefinitionBuilder
				.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));

		registerInfrastructureBeanWithId(listenerBeanDefinitionBuilder.getBeanDefinition(),
				ReactiveAuditingEntityCallback.class.getName(), registry);
	}

	static void potentiallyRegisterR2dbcPersistentEntities(BeanDefinitionBuilder builder,
			BeanDefinitionRegistry registry) {

		String persistentEntitiesBeanName = R2dbcAuditingRegistrar.detectPersistentEntitiesBeanName(registry);

		if (persistentEntitiesBeanName == null) {

			persistentEntitiesBeanName = BeanDefinitionReaderUtils.uniqueBeanName("r2dbcPersistentEntities", registry);

			// TODO: https://github.com/spring-projects/spring-framework/issues/28728
			BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntities.class) //
					.setFactoryMethod("of") //
					.addConstructorArgReference("r2dbcMappingContext");

			registry.registerBeanDefinition(persistentEntitiesBeanName, definition.getBeanDefinition());
		}

		builder.addConstructorArgReference(persistentEntitiesBeanName);
	}

	@Nullable
	private static String detectPersistentEntitiesBeanName(BeanDefinitionRegistry registry) {

		if (registry instanceof ListableBeanFactory beanFactory) {
			for (String bn : beanFactory.getBeanNamesForType(PersistentEntities.class)) {
				if (bn.startsWith("r2dbc")) {
					return bn;
				}
			}
		}

		return null;
	}

}
