package org.springframework.data.r2dbc.repository.config;

import io.r2dbc.spi.ConnectionFactory;

import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.data.r2dbc.support.SqlErrorCodeR2dbcExceptionTranslator;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Base class for Spring Data R2DBC configuration containing bean declarations that must be registered for Spring Data
 * R2DBC to work.
 *
 * @author Mark Paluch
 * @see ConnectionFactory
 * @see DatabaseClient
 * @see EnableR2dbcRepositories
 */
@Configuration
public abstract class AbstractR2dbcConfiguration {

	/**
	 * Return a R2DBC {@link ConnectionFactory}. Annotate with {@link Bean} in case you want to expose a
	 * {@link ConnectionFactory} instance to the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return the configured {@link ConnectionFactory}.
	 */
	public abstract ConnectionFactory connectionFactory();

	/**
	 * Register a {@link DatabaseClient} using {@link #connectionFactory()} and {@link RelationalMappingContext}.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean
	public DatabaseClient databaseClient(ReactiveDataAccessStrategy dataAccessStrategy,
			R2dbcExceptionTranslator exceptionTranslator) {

		return DatabaseClient.builder().connectionFactory(connectionFactory()).dataAccessStrategy(dataAccessStrategy)
				.exceptionTranslator(exceptionTranslator).build();
	}

	/**
	 * Register a {@link RelationalMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link NamingStrategy#INSTANCE} as fallback.
	 * @return must not be {@literal null}.
	 */
	@Bean
	public RelationalMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy) {
		return new RelationalMappingContext(namingStrategy.orElse(NamingStrategy.INSTANCE));
	}

	/**
	 * Creates a {@link ReactiveDataAccessStrategy} using the configured {@link #r2dbcMappingContext(Optional)}.
	 *
	 * @param mappingContext the configured {@link RelationalMappingContext}.
	 * @return must not be {@literal null}.
	 * @see #r2dbcMappingContext(Optional)
	 */
	@Bean
	public ReactiveDataAccessStrategy reactiveDataAccessStrategy(RelationalMappingContext mappingContext) {
		return new DefaultReactiveDataAccessStrategy(new BasicRelationalConverter(mappingContext));
	}

	/**
	 * Creates a {@link R2dbcExceptionTranslator} using the configured {@link #connectionFactory()}.
	 *
	 * @return must not be {@literal null}.
	 * @see #connectionFactory()
	 */
	@Bean
	public R2dbcExceptionTranslator exceptionTranslator() {
		return new SqlErrorCodeR2dbcExceptionTranslator(connectionFactory());
	}
}
