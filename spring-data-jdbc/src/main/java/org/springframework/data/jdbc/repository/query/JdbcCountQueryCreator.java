/*
 * Copyright 2021-2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.util.Optional;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link JdbcQueryCreator} that creates {@code COUNT(*)} queries without applying limit/offset and {@link Sort}.
 *
 * @author Mark Paluch
 * @author Diego Krupitza
 * @since 2.2
 */
public class JdbcCountQueryCreator extends JdbcQueryCreator {

	public JdbcCountQueryCreator(PartTree tree, JdbcConverter converter, Dialect dialect, JdbcQueryMethod queryMethod,
			RelationalParameterAccessor accessor, ReturnedType returnedType) {
		super(tree, converter, dialect, queryMethod, accessor, returnedType);
	}

	JdbcCountQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor, boolean isSliceQuery,
			ReturnedType returnedType, Optional<Lock> lockMode) {
		super(context, tree, converter, dialect, entityMetadata, accessor, isSliceQuery, returnedType, lockMode);
	}

	@Override
	StatementFactory.SelectionBuilder getSelection(RelationalPersistentEntity<?> entity) {
		return getStatementFactory().count(entity);
	}
}
