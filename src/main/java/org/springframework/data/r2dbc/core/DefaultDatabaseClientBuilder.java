/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import java.util.function.Consumer;

import org.springframework.data.r2dbc.core.DatabaseClient.Builder;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.support.R2dbcExceptionSubclassTranslator;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link DatabaseClient.Builder}.
 *
 * @author Mark Paluch
 */
class DefaultDatabaseClientBuilder implements DatabaseClient.Builder {

	private @Nullable ConnectionFactory connectionFactory;

	private @Nullable R2dbcExceptionTranslator exceptionTranslator;

	private ReactiveDataAccessStrategy accessStrategy;

	private boolean namedParameters = true;

	DefaultDatabaseClientBuilder() {}

	DefaultDatabaseClientBuilder(DefaultDatabaseClientBuilder other) {

		Assert.notNull(other, "DefaultDatabaseClientBuilder must not be null!");

		this.connectionFactory = other.connectionFactory;
		this.exceptionTranslator = other.exceptionTranslator;
		this.accessStrategy = other.accessStrategy;
		this.namedParameters = other.namedParameters;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#connectionFactory(io.r2dbc.spi.ConnectionFactory)
	 */
	@Override
	public Builder connectionFactory(ConnectionFactory factory) {

		Assert.notNull(factory, "ConnectionFactory must not be null!");

		this.connectionFactory = factory;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#exceptionTranslator(org.springframework.data.r2dbc.support.R2dbcExceptionTranslator)
	 */
	@Override
	public Builder exceptionTranslator(R2dbcExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "R2dbcExceptionTranslator must not be null!");

		this.exceptionTranslator = exceptionTranslator;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#dataAccessStrategy(org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy)
	 */
	@Override
	public Builder dataAccessStrategy(ReactiveDataAccessStrategy accessStrategy) {

		Assert.notNull(accessStrategy, "ReactiveDataAccessStrategy must not be null!");

		this.accessStrategy = accessStrategy;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#namedParameters(boolean)
	 */
	@Override
	public Builder namedParameters(boolean enabled) {

		this.namedParameters = enabled;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#build()
	 */
	@Override
	public DatabaseClient build() {

		R2dbcExceptionTranslator exceptionTranslator = this.exceptionTranslator;

		if (exceptionTranslator == null) {
			exceptionTranslator = new R2dbcExceptionSubclassTranslator();
		}

		ReactiveDataAccessStrategy accessStrategy = this.accessStrategy;

		if (accessStrategy == null) {

			R2dbcDialect dialect = DialectResolver.getDialect(this.connectionFactory);
			accessStrategy = new DefaultReactiveDataAccessStrategy(dialect);
		}

		return doBuild(this.connectionFactory, exceptionTranslator, accessStrategy, namedParameters,
				new DefaultDatabaseClientBuilder(this));
	}

	protected DatabaseClient doBuild(ConnectionFactory connector, R2dbcExceptionTranslator exceptionTranslator,
			ReactiveDataAccessStrategy accessStrategy, boolean namedParameters, DefaultDatabaseClientBuilder builder) {
		return new DefaultDatabaseClient(connector, exceptionTranslator, accessStrategy, namedParameters, builder);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public DatabaseClient.Builder clone() {
		return new DefaultDatabaseClientBuilder(this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.DatabaseClient.Builder#apply(java.util.function.Consumer)
	 */
	@Override
	public DatabaseClient.Builder apply(Consumer<DatabaseClient.Builder> builderConsumer) {
		Assert.notNull(builderConsumer, "BuilderConsumer must not be null");

		builderConsumer.accept(this);
		return this;
	}
}
