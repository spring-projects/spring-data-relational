/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory.lookup;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link AbstractRoutingConnectionFactory}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
class AbstractRoutingConnectionFactoryUnitTests {

	private static final String ROUTING_KEY = "routingKey";

	@Mock ConnectionFactory defaultConnectionFactory;
	@Mock ConnectionFactory routedConnectionFactory;

	private DummyRoutingConnectionFactory connectionFactory;

	@BeforeEach
	void before() {

		connectionFactory = new DummyRoutingConnectionFactory();
		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
	}

	@Test // gh-98
	void shouldDetermineRoutedFactory() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", routedConnectionFactory));
		connectionFactory.setConnectionFactoryLookup(new MapConnectionFactoryLookup());
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "key")) //
				.as(StepVerifier::create) //
				.expectNext(routedConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	void shouldFallbackToDefaultConnectionFactory() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", routedConnectionFactory));
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	void initializationShouldFailUnsupportedLookupKey() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", new Object()));

		assertThatThrownBy(() -> connectionFactory.afterPropertiesSet()).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // gh-98
	void initializationShouldFailUnresolvableKey() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", "value"));
		connectionFactory.setConnectionFactoryLookup(new MapConnectionFactoryLookup());

		assertThatThrownBy(() -> connectionFactory.afterPropertiesSet()) //
				.isInstanceOf(ConnectionFactoryLookupFailureException.class) //
				.hasMessageContaining("No ConnectionFactory with name 'value' registered");
	}

	@Test // gh-98
	void unresolvableConnectionFactoryRetrievalShouldFail() {

		connectionFactory.setLenientFallback(false);
		connectionFactory.setConnectionFactoryLookup(new MapConnectionFactoryLookup());
		connectionFactory.setTargetConnectionFactories(singletonMap("key", routedConnectionFactory));
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "unknown")) //
				.as(StepVerifier::create) //
				.verifyError(IllegalStateException.class);
	}

	@Test // gh-98
	void connectionFactoryRetrievalWithUnknownLookupKeyShouldReturnDefaultConnectionFactory() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", routedConnectionFactory));
		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "unknown")) //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	void connectionFactoryRetrievalWithoutLookupKeyShouldReturnDefaultConnectionFactory() {

		connectionFactory.setTargetConnectionFactories(singletonMap("key", routedConnectionFactory));
		connectionFactory.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		connectionFactory.setLenientFallback(false);
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	void shouldLookupFromMap() {

		MapConnectionFactoryLookup lookup = new MapConnectionFactoryLookup("lookup-key", routedConnectionFactory);

		connectionFactory.setConnectionFactoryLookup(lookup);
		connectionFactory.setTargetConnectionFactories(singletonMap("my-key", "lookup-key"));
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "my-key")) //
				.as(StepVerifier::create) //
				.expectNext(routedConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	void shouldAllowModificationsAfterInitialization() {

		MapConnectionFactoryLookup lookup = new MapConnectionFactoryLookup();

		connectionFactory.setConnectionFactoryLookup(lookup);
		connectionFactory.setTargetConnectionFactories(lookup.getConnectionFactories());
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "lookup-key")) //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();

		lookup.addConnectionFactory("lookup-key", routedConnectionFactory);
		connectionFactory.afterPropertiesSet();

		connectionFactory.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "lookup-key")) //
				.as(StepVerifier::create) //
				.expectNext(routedConnectionFactory) //
				.verifyComplete();
	}

	static class DummyRoutingConnectionFactory extends AbstractRoutingConnectionFactory {

		@Override
		protected Mono<Object> determineCurrentLookupKey() {
			return Mono.subscriberContext().filter(it -> it.hasKey(ROUTING_KEY)).map(it -> it.get(ROUTING_KEY));
		}
	}
}
