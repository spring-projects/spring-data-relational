/*
 * Copyright 2019-2020 the original author or authors.
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
import static org.mockito.Mockito.*;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import org.junit.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Tests for {@link AbstractR2dbcConfiguration}.
 *
 * @author Mark Paluch
 */
public class R2dbcConfigurationIntegrationTests {

	@Test // gh-95
	public void shouldLookupConnectionFactoryThroughLocalCall() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				NonBeanConnectionFactoryConfiguration.class);

		context.getBean(DatabaseClient.class);

		NonBeanConnectionFactoryConfiguration bean = context.getBean(NonBeanConnectionFactoryConfiguration.class);

		assertThat(context.getBeanNamesForType(ConnectionFactory.class)).isEmpty();
		assertThat(bean.callCounter).isGreaterThan(2);

		context.stop();
	}

	@Test // gh-95
	public void shouldLookupConnectionFactoryThroughLocalCallForExistingCustomBeans() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				CustomConnectionFactoryBeanNameConfiguration.class);

		context.getBean(DatabaseClient.class);

		CustomConnectionFactoryBeanNameConfiguration bean = context
				.getBean(CustomConnectionFactoryBeanNameConfiguration.class);

		assertThat(context.getBeanNamesForType(ConnectionFactory.class)).hasSize(1).contains("myCustomBean");
		assertThat(bean.callCounter).isGreaterThan(2);

		ConnectionFactoryWrapper wrapper = context.getBean(ConnectionFactoryWrapper.class);
		assertThat(wrapper.connectionFactory).isExactlyInstanceOf(H2ConnectionFactory.class);

		context.stop();
	}

	@Test // gh-95
	public void shouldRegisterConnectionFactory() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				BeanConnectionFactoryConfiguration.class);

		context.getBean(DatabaseClient.class);

		BeanConnectionFactoryConfiguration bean = context.getBean(BeanConnectionFactoryConfiguration.class);

		assertThat(bean.callCounter).isEqualTo(1);
		assertThat(context.getBeanNamesForType(ConnectionFactory.class)).hasSize(1);

		context.stop();
	}

	@Configuration(proxyBeanMethods = false)
	static class NonBeanConnectionFactoryConfiguration extends AbstractR2dbcConfiguration {

		int callCounter;

		@Override
		public ConnectionFactory connectionFactory() {

			callCounter++;
			return new H2ConnectionFactory(
					H2ConnectionConfiguration.builder().inMemory("foo").username("sa").password("").build());
		}
	}

	@Configuration(proxyBeanMethods = false)
	static class CustomConnectionFactoryBeanNameConfiguration extends AbstractR2dbcConfiguration {

		int callCounter;

		@Bean
		public ConnectionFactory myCustomBean() {
			return mock(ConnectionFactory.class);
		}

		@Override
		public ConnectionFactory connectionFactory() {

			callCounter++;
			return new H2ConnectionFactory(
					H2ConnectionConfiguration.builder().inMemory("foo").username("sa").password("").build());
		}

		@Bean
		ConnectionFactoryWrapper wrapper() {
			return new ConnectionFactoryWrapper(lookupConnectionFactory());
		}
	}

	static class ConnectionFactoryWrapper {
		ConnectionFactory connectionFactory;

		ConnectionFactoryWrapper(ConnectionFactory connectionFactory) {
			this.connectionFactory = connectionFactory;
		}
	}

	@Configuration(proxyBeanMethods = false)
	static class BeanConnectionFactoryConfiguration extends NonBeanConnectionFactoryConfiguration {

		@Override
		@Bean
		public ConnectionFactory connectionFactory() {
			return super.connectionFactory();
		}
	}

}
