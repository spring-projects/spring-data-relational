/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.IdentifierProcessing;

/**
 * {@link RelationalConverter} that uses a {@link MappingContext} to apply conversion of relational values to property
 * values.
 * <p>
 * Conversion is configurable by providing a customized {@link CustomConversions}.
 *
 * @author Mark Paluch
 * @since 1.1
 * @deprecated since 3.2, use {@link MappingJdbcConverter} instead as the naming suggests a limited scope of
 *             functionality.
 */
@Deprecated(since = "3.2")
public class BasicJdbcConverter extends MappingJdbcConverter {

	/**
	 * Creates a new {@link BasicJdbcConverter} given {@link MappingContext} and a {@link JdbcTypeFactory#unsupported()
	 * no-op type factory} throwing {@link UnsupportedOperationException} on type creation. Use
	 * {@link #BasicJdbcConverter(RelationalMappingContext, RelationResolver, CustomConversions, JdbcTypeFactory, IdentifierProcessing)}
	 * (MappingContext, RelationResolver, JdbcTypeFactory)} to convert arrays and large objects into JDBC-specific types.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 */
	public BasicJdbcConverter(RelationalMappingContext context, RelationResolver relationResolver) {
		super(context, relationResolver);
	}

	/**
	 * Creates a new {@link BasicJdbcConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @since 3.2
	 */
	public BasicJdbcConverter(RelationalMappingContext context, RelationResolver relationResolver,
			CustomConversions conversions, JdbcTypeFactory typeFactory) {
		super(context, relationResolver, conversions, typeFactory);
	}

	/**
	 * Creates a new {@link BasicJdbcConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @param relationResolver used to fetch additional relations from the database. Must not be {@literal null}.
	 * @param typeFactory must not be {@literal null}
	 * @param identifierProcessing must not be {@literal null}
	 * @since 2.0
	 */
	public BasicJdbcConverter(RelationalMappingContext context, RelationResolver relationResolver,
			CustomConversions conversions, JdbcTypeFactory typeFactory, IdentifierProcessing identifierProcessing) {
		super(context, relationResolver, conversions, typeFactory);
	}
}
