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
package org.springframework.data.jdbc.core;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.jdbc.core.conversion.DbAction;
import org.springframework.data.jdbc.core.conversion.DbAction.Delete;
import org.springframework.data.jdbc.core.conversion.DbAction.DeleteAll;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.conversion.Interpreter;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.Assert;

/**
 * {@link Interpreter} for {@link DbAction}s using a {@link DataAccessStrategy} for performing actual database
 * interactions.
 *
 * @author Jens Schauder
 * @since 1.0
 */
class DefaultJdbcInterpreter implements Interpreter {

	private final JdbcMappingContext context;
	private final DataAccessStrategy accessStrategy;

	DefaultJdbcInterpreter(JdbcMappingContext context, DataAccessStrategy accessStrategy) {

		this.context = context;
		this.accessStrategy = accessStrategy;
	}

	@Override
	public <T> void interpret(Insert<T> insert) {
		accessStrategy.insert(insert.getEntity(), insert.getEntityType(), createAdditionalColumnValues(insert));
	}

	@Override
	public <T> void interpret(Update<T> update) {
		accessStrategy.update(update.getEntity(), update.getEntityType());
	}

	@Override
	public <T> void interpret(Delete<T> delete) {

		if (delete.getPropertyPath() == null) {
			accessStrategy.delete(delete.getRootId(), delete.getEntityType());
		} else {
			accessStrategy.delete(delete.getRootId(), delete.getPropertyPath().getPath());
		}
	}

	@Override
	public <T> void interpret(DeleteAll<T> delete) {

		if (delete.getEntityType() == null) {
			accessStrategy.deleteAll(delete.getPropertyPath().getPath());
		} else {
			accessStrategy.deleteAll(delete.getEntityType());
		}
	}

	private <T> Map<String, Object> createAdditionalColumnValues(Insert<T> insert) {

		Map<String, Object> additionalColumnValues = new HashMap<>();
		addDependingOnInformation(insert, additionalColumnValues);
		additionalColumnValues.putAll(insert.getAdditionalValues());

		return additionalColumnValues;
	}

	private <T> void addDependingOnInformation(Insert<T> insert, Map<String, Object> additionalColumnValues) {

		DbAction dependingOn = insert.getDependingOn();

		if (dependingOn == null) {
			return;
		}

		JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(dependingOn.getEntityType());

		String columnName = getColumnNameForReverseColumn(insert, persistentEntity);

		Object identifier = getIdFromEntityDependingOn(dependingOn, persistentEntity);

		additionalColumnValues.put(columnName, identifier);
	}

	private Object getIdFromEntityDependingOn(DbAction dependingOn, JdbcPersistentEntity<?> persistentEntity) {
		return persistentEntity.getIdentifierAccessor(dependingOn.getEntity()).getIdentifier();
	}

	private <T> String getColumnNameForReverseColumn(Insert<T> insert, JdbcPersistentEntity<?> persistentEntity) {

		PropertyPath path = insert.getPropertyPath().getPath();

		Assert.notNull(path, "There shouldn't be an insert depending on another insert without having a PropertyPath.");

		return persistentEntity.getRequiredPersistentProperty(path.getSegment()).getReverseColumnName();
	}
}
