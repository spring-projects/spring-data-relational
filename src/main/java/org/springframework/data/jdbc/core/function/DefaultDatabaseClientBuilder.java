/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.data.jdbc.core.function;

import java.util.function.Consumer;

import org.springframework.data.jdbc.core.function.DatabaseClient.Builder;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import io.r2dbc.spi.ConnectionFactory;

/**
 * Default implementation of {@link DatabaseClient.Builder}.
 *
 * @author Mark Paluch
 */
class DefaultDatabaseClientBuilder implements DatabaseClient.Builder {

	private @Nullable ConnectionFactory connector;
	private SQLExceptionTranslator exceptionTranslator = new SQLErrorCodeSQLExceptionTranslator();
	private ReactiveDataAccessStrategy accessStrategy = new DefaultReactiveDataAccessStrategy();

	DefaultDatabaseClientBuilder() {}

	DefaultDatabaseClientBuilder(DefaultDatabaseClientBuilder other) {

		Assert.notNull(other, "DefaultDatabaseClientBuilder must not be null!");

		this.connector = other.connector;
		this.exceptionTranslator = exceptionTranslator;
	}

	@Override
	public Builder connectionFactory(ConnectionFactory factory) {

		Assert.notNull(factory, "ConnectionFactory must not be null!");

		this.connector = factory;
		return this;
	}

	@Override
	public Builder exceptionTranslator(SQLExceptionTranslator exceptionTranslator) {

		Assert.notNull(exceptionTranslator, "SQLExceptionTranslator must not be null!");

		this.exceptionTranslator = exceptionTranslator;
		return this;
	}

	@Override
	public Builder dataAccessStrategy(ReactiveDataAccessStrategy accessStrategy) {

		Assert.notNull(accessStrategy, "ReactiveDataAccessStrategy must not be null!");

		this.accessStrategy = accessStrategy;
		return this;
	}

	@Override
	public DatabaseClient build() {

		return new DefaultDatabaseClient(this.connector, exceptionTranslator, accessStrategy,
				new DefaultDatabaseClientBuilder(this));
	}

	@Override
	public DatabaseClient.Builder clone() {
		return new DefaultDatabaseClientBuilder(this);
	}

	@Override
	public DatabaseClient.Builder apply(Consumer<DatabaseClient.Builder> builderConsumer) {
		Assert.notNull(builderConsumer, "BuilderConsumer must not be null");

		builderConsumer.accept(this);
		return this;
	}
}
