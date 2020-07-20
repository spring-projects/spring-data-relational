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
package org.springframework.data.r2dbc.connectionfactory.init;

import io.r2dbc.spi.ConnectionFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Used to {@link #setDatabasePopulator set up} a database during initialization and {@link #setDatabaseCleaner clean
 * up} a database during destruction.
 *
 * @author Mark Paluch
 * @see DatabasePopulator
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@Deprecated
public class ConnectionFactoryInitializer implements InitializingBean, DisposableBean {

	private @Nullable ConnectionFactory connectionFactory;

	private @Nullable DatabasePopulator databasePopulator;

	private @Nullable DatabasePopulator databaseCleaner;

	private boolean enabled = true;

	/**
	 * The {@link ConnectionFactory} for the database to populate when this component is initialized and to clean up when
	 * this component is shut down.
	 * <p/>
	 * This property is mandatory with no default provided.
	 *
	 * @param connectionFactory the R2DBC {@link ConnectionFactory}.
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * Set the {@link DatabasePopulator} to execute during the bean initialization phase.
	 *
	 * @param databasePopulator the {@link DatabasePopulator} to use during initialization
	 * @see #setDatabaseCleaner
	 */
	public void setDatabasePopulator(DatabasePopulator databasePopulator) {
		this.databasePopulator = databasePopulator;
	}

	/**
	 * Set the {@link DatabasePopulator} to execute during the bean destruction phase, cleaning up the database and
	 * leaving it in a known state for others.
	 *
	 * @param databaseCleaner the {@link DatabasePopulator} to use during destruction
	 * @see #setDatabasePopulator
	 */
	public void setDatabaseCleaner(DatabasePopulator databaseCleaner) {
		this.databaseCleaner = databaseCleaner;
	}

	/**
	 * Flag to explicitly enable or disable the {@link #setDatabasePopulator database populator} and
	 * {@link #setDatabaseCleaner database cleaner}.
	 *
	 * @param enabled {@literal true} if the database populator and database cleaner should be called on startup and
	 *          shutdown, respectively
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	/**
	 * Use the {@link #setDatabasePopulator database populator} to set up the database.
	 */
	@Override
	public void afterPropertiesSet() {
		execute(this.databasePopulator);
	}

	/**
	 * Use the {@link #setDatabaseCleaner database cleaner} to clean up the database.
	 */
	@Override
	public void destroy() {
		execute(this.databaseCleaner);
	}

	private void execute(@Nullable DatabasePopulator populator) {

		Assert.state(this.connectionFactory != null, "ConnectionFactory must be set");

		if (this.enabled && populator != null) {
			DatabasePopulatorUtils.execute(populator, this.connectionFactory).block();
		}
	}
}
