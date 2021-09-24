/*
 * Copyright 2018-2021 the original author or authors.
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

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.relational.core.mapping.event.RelationalAuditingCallback;
import org.springframework.data.repository.config.PersistentEntitiesFactoryBean;
import org.springframework.util.Assert;

/**
 * {@link ImportBeanDefinitionRegistrar} which registers additional beans in order to enable auditing via the
 * {@link EnableJdbcAuditing} annotation.
 *
 * @see EnableJdbcAuditing
 * @author Kazuki Shimizu
 * @author Jens Schauder
 */
class JdbcAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport {

	private static final String AUDITING_HANDLER_BEAN_NAME = "jdbcAuditingHandler";
	private static final String JDBC_MAPPING_CONTEXT_BEAN_NAME = "jdbcMappingContext";

	/**
	 * {@inheritDoc}
	 *
	 * @return return the {@link EnableJdbcAuditing}
	 * @see AuditingBeanDefinitionRegistrarSupport#getAnnotation()
	 */
	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableJdbcAuditing.class;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return return "{@literal jdbcAuditingHandler}"
	 * @see AuditingBeanDefinitionRegistrarSupport#getAuditingHandlerBeanName()
	 */
	@Override
	protected String getAuditingHandlerBeanName() {
		return AUDITING_HANDLER_BEAN_NAME;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditHandlerBeanDefinitionBuilder(org.springframework.data.auditing.config.AuditingConfiguration)
	 */
	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {

		Assert.notNull(configuration, "AuditingConfiguration must not be null!");

		BeanDefinitionBuilder builder = configureDefaultAuditHandlerAttributes(configuration,
				BeanDefinitionBuilder.rootBeanDefinition(IsNewAwareAuditingHandler.class));


		BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntitiesFactoryBean.class);
		definition.addConstructorArgReference(JDBC_MAPPING_CONTEXT_BEAN_NAME);

		return builder.addConstructorArgValue(definition.getBeanDefinition());
	}

	/**
	 * Register the bean definition of {@link RelationalAuditingCallback}. {@inheritDoc}
	 *
	 * @see AuditingBeanDefinitionRegistrarSupport#registerAuditListenerBeanDefinition(BeanDefinition,
	 *      BeanDefinitionRegistry)
	 */
	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {

		Class<?> listenerClass = RelationalAuditingCallback.class;
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(listenerClass) //
				.addConstructorArgReference(AUDITING_HANDLER_BEAN_NAME);

		registerInfrastructureBeanWithId(builder.getRawBeanDefinition(), listenerClass.getName(), registry);
	}

}
