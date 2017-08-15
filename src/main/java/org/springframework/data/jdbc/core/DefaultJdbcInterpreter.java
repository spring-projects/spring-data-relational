/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.jdbc.core.conversion.DbAction;
import org.springframework.data.jdbc.core.conversion.DbAction.Delete;
import org.springframework.data.jdbc.core.conversion.DbAction.DeleteAll;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.core.conversion.Interpreter;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;

/**
 * {@link Interpreter} for {@link DbAction}s using a {@link JdbcEntityTemplate} for performing actual database
 * interactions.
 *
 * @author Jens Schauder
 */
class DefaultJdbcInterpreter implements Interpreter {

	private final JdbcMappingContext context;
	private final JdbcEntityTemplate template;

	DefaultJdbcInterpreter(JdbcMappingContext context, JdbcEntityTemplate template) {

		this.context = context;
		this.template = template;
	}

	@Override
	public <T> void interpret(Insert<T> insert) {

		Map<String, Object> additionalColumnValues = new HashMap<>();
		DbAction dependingOn = insert.getDependingOn();

		if (dependingOn != null) {

			JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(dependingOn.getEntityType());
			String columnName = persistentEntity.getTableName();
			Object entity = dependingOn.getEntity();
			Object identifier = persistentEntity.getIdentifierAccessor(entity).getIdentifier();

			additionalColumnValues.put(columnName, identifier);
		}

		template.insert(insert.getEntity(), insert.getEntityType(), additionalColumnValues);
	}

	@Override
	public <T> void interpret(Update<T> update) {
		template.update(update.getEntity(), update.getEntityType());
	}

	@Override
	public <T> void interpret(Delete<T> delete) {

		if (delete.getPropertyPath() == null) {
			template.doDelete(delete.getRootId(), delete.getEntityType());
		} else {
			template.doDelete(delete.getRootId(), delete.getPropertyPath());
		}

	}

	@Override
	public <T> void interpret(DeleteAll<T> delete) {
		template.doDeleteAll(delete.getEntityType(), delete.getPropertyPath());
	}
}
