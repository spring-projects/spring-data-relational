/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.ConnectionFactory;

import java.util.function.Consumer;

import org.springframework.data.r2dbc.function.DatabaseClient.Builder;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
class DefaultTransactionalDatabaseClientBuilder extends DefaultDatabaseClientBuilder
		implements TransactionalDatabaseClient.Builder {

	DefaultTransactionalDatabaseClientBuilder() {}

	DefaultTransactionalDatabaseClientBuilder(DefaultDatabaseClientBuilder other) {

		super(other);
		Assert.notNull(other, "DefaultDatabaseClientBuilder must not be null!");
	}

	@Override
	public DatabaseClient.Builder clone() {
		return new DefaultTransactionalDatabaseClientBuilder(this);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#connectionFactory(io.r2dbc.spi.ConnectionFactory)
	 */
	@Override
	public TransactionalDatabaseClient.Builder connectionFactory(ConnectionFactory factory) {
		super.connectionFactory(factory);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#exceptionTranslator(org.springframework.data.r2dbc.support.R2dbcExceptionTranslator)
	 */
	@Override
	public TransactionalDatabaseClient.Builder exceptionTranslator(R2dbcExceptionTranslator exceptionTranslator) {
		super.exceptionTranslator(exceptionTranslator);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#dataAccessStrategy(org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy)
	 */
	@Override
	public TransactionalDatabaseClient.Builder dataAccessStrategy(ReactiveDataAccessStrategy accessStrategy) {
		super.dataAccessStrategy(accessStrategy);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#dataAccessStrategy(org.springframework.data.r2dbc.function.NamedParameterSupport)
	 */
	@Override
	public TransactionalDatabaseClient.Builder namedParameters(NamedParameterExpander namedParameters) {
		super.namedParameters(namedParameters);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#apply(java.util.function.Consumer)
	 */
	@Override
	public TransactionalDatabaseClient.Builder apply(Consumer<Builder> builderConsumer) {
		super.apply(builderConsumer);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DefaultDatabaseClientBuilder#build()
	 */
	@Override
	public TransactionalDatabaseClient build() {
		return (TransactionalDatabaseClient) super.build();
	}

	@Override
	protected DatabaseClient doBuild(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
			ReactiveDataAccessStrategy accessStrategy, NamedParameterExpander namedParameters,
			DefaultDatabaseClientBuilder builder) {
		return new DefaultTransactionalDatabaseClient(connector, exceptionTranslator, accessStrategy, namedParameters,
				builder);
	}
}
