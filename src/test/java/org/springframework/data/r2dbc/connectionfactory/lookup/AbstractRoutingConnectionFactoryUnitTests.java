/*
 * Copyright 2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link AbstractRoutingConnectionFactory}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractRoutingConnectionFactoryUnitTests {

	private static final String ROUTING_KEY = "routingKey";

	@Mock ConnectionFactory defaultConnectionFactory;
	@Mock ConnectionFactory routedConnectionFactory;

	DummyRoutingConnectionFactory sut;

	@Before
	public void before() {

		sut = new DummyRoutingConnectionFactory();
		sut.setDefaultTargetConnectionFactory(defaultConnectionFactory);
	}

	@Test // gh-98
	public void shouldDetermineRoutedFactory() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", routedConnectionFactory));
		sut.setConnectionFactoryLookup(new MapConnectionFactoryLookup());
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "key")) //
				.as(StepVerifier::create) //
				.expectNext(routedConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	public void shouldFallbackToDefaultConnectionFactory() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", routedConnectionFactory));
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	public void initializationShouldFailUnsupportedLookupKey() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", new Object()));

		assertThatThrownBy(() -> sut.afterPropertiesSet()).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // gh-98
	public void initializationShouldFailUnresolvableKey() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", "value"));
		sut.setConnectionFactoryLookup(new MapConnectionFactoryLookup());

		assertThatThrownBy(() -> sut.afterPropertiesSet()).isInstanceOf(ConnectionFactoryLookupFailureException.class)
				.hasMessageContaining("No ConnectionFactory with name 'value' registered");
	}

	@Test // gh-98
	public void unresolvableConnectionFactoryRetrievalShouldFail() {

		sut.setLenientFallback(false);
		sut.setConnectionFactoryLookup(new MapConnectionFactoryLookup());
		sut.setTargetConnectionFactories(Collections.singletonMap("key", routedConnectionFactory));
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "unknown")) //
				.as(StepVerifier::create) //
				.verifyError(IllegalStateException.class);
	}

	@Test // gh-98
	public void connectionFactoryRetrievalWithUnknownLookupKeyShouldReturnDefaultConnectionFactory() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", routedConnectionFactory));
		sut.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "unknown")) //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	public void connectionFactoryRetrievalWithoutLookupKeyShouldReturnDefaultConnectionFactory() {

		sut.setTargetConnectionFactories(Collections.singletonMap("key", routedConnectionFactory));
		sut.setDefaultTargetConnectionFactory(defaultConnectionFactory);
		sut.setLenientFallback(false);
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	public void shouldLookupFromMap() {

		MapConnectionFactoryLookup lookup = new MapConnectionFactoryLookup("lookup-key", routedConnectionFactory);

		sut.setConnectionFactoryLookup(lookup);
		sut.setTargetConnectionFactories(Collections.singletonMap("my-key", "lookup-key"));
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "my-key")) //
				.as(StepVerifier::create) //
				.expectNext(routedConnectionFactory) //
				.verifyComplete();
	}

	@Test // gh-98
	@SuppressWarnings("unchecked")
	public void shouldAllowModificationsAfterInitialization() {

		MapConnectionFactoryLookup lookup = new MapConnectionFactoryLookup();

		sut.setConnectionFactoryLookup(lookup);
		sut.setTargetConnectionFactories((Map) lookup.getConnectionFactories());
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
				.subscriberContext(Context.of(ROUTING_KEY, "lookup-key")) //
				.as(StepVerifier::create) //
				.expectNext(defaultConnectionFactory) //
				.verifyComplete();

		lookup.addConnectionFactory("lookup-key", routedConnectionFactory);
		sut.afterPropertiesSet();

		sut.determineTargetConnectionFactory() //
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
