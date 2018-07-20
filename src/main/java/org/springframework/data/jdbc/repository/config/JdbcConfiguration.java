/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Beans that must be registered for Spring Data JDBC to work.
 *
 * @author Greg Turnquist
 * @author Jens Schauder
 * @author Mark Paluch
 */
@Configuration
public class JdbcConfiguration {

	@Bean
	protected RelationalMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy,
			CustomConversions customConversions) {

		RelationalMappingContext mappingContext = new RelationalMappingContext(
				namingStrategy.orElse(NamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
	}

	@Bean
	protected RelationalConverter relationalConverter(RelationalMappingContext mappingContext,
			CustomConversions customConversions) {
		return new BasicRelationalConverter(mappingContext, customConversions);
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the
	 * {@link #relationalConverter(RelationalMappingContext, CustomConversions)}. Returns an empty
	 * {@link JdbcCustomConversions} instance by default.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean
	protected CustomConversions jdbcCustomConversions() {
		return new JdbcCustomConversions();
	}
}
