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

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAll;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAllRoot;
import org.springframework.data.relational.core.conversion.DbAction.DeleteRoot;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.conversion.DbAction.Merge;
import org.springframework.data.relational.core.conversion.DbAction.Update;
import org.springframework.data.relational.core.conversion.DbAction.UpdateRoot;
import org.springframework.data.relational.core.conversion.Interpreter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * {@link Interpreter} for {@link DbAction}s using a {@link DataAccessStrategy} for performing actual database
 * interactions.
 *
 * @author Jens Schauder
 * @since 1.0
 */
class DefaultJdbcInterpreter implements Interpreter {

	private final RelationalMappingContext context;
	private final DataAccessStrategy accessStrategy;

	DefaultJdbcInterpreter(RelationalMappingContext context, DataAccessStrategy accessStrategy) {

		this.context = context;
		this.accessStrategy = accessStrategy;
	}

	@Override
	public <T> void interpret(Insert<T> insert) {
		accessStrategy.insert(insert.getEntity(), insert.getEntityType(), createAdditionalColumnValues(insert));
	}

	@Override
	public <T> void interpret(InsertRoot<T> insert) {
		accessStrategy.insert(insert.getEntity(), insert.getEntityType(), new HashMap<>());
	}

	@Override
	public <T> void interpret(Update<T> update) {
		accessStrategy.update(update.getEntity(), update.getEntityType());
	}

	@Override
	public <T> void interpret(UpdateRoot<T> update) {
		accessStrategy.update(update.getEntity(), update.getEntityType());
	}

	@Override
	public <T> void interpret(Merge<T> merge) {

		// temporary implementation
		if (!accessStrategy.update(merge.getEntity(), merge.getEntityType())) {
			accessStrategy.insert(merge.getEntity(), merge.getEntityType(), createAdditionalColumnValues(merge));
		}
	}

	@Override
	public <T> void interpret(Delete<T> delete) {
		accessStrategy.delete(delete.getRootId(), delete.getPropertyPath());
	}

	@Override
	public <T> void interpret(DeleteRoot<T> delete) {
		accessStrategy.delete(delete.getRootId(), delete.getEntityType());
	}

	@Override
	public <T> void interpret(DeleteAll<T> delete) {
		accessStrategy.deleteAll(delete.getPropertyPath());
	}

	@Override
	public <T> void interpret(DeleteAllRoot<T> deleteAllRoot) {
		accessStrategy.deleteAll(deleteAllRoot.getEntityType());
	}

	private <T> Map<String, Object> createAdditionalColumnValues(DbAction.WithDependingOn<T> action) {

		Map<String, Object> additionalColumnValues = new HashMap<>();
		addDependingOnInformation(action, additionalColumnValues);
		additionalColumnValues.putAll(action.getAdditionalValues());

		return additionalColumnValues;
	}

	private <T> void addDependingOnInformation(DbAction.WithDependingOn<T> action, Map<String, Object> additionalColumnValues) {

		DbAction.WithEntity<?> dependingOn = action.getDependingOn();

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(dependingOn.getEntityType());

		String columnName = getColumnNameForReverseColumn(action);

		Object identifier = getIdFromEntityDependingOn(dependingOn, persistentEntity);

		additionalColumnValues.put(columnName, identifier);
	}

	@Nullable
	private Object getIdFromEntityDependingOn(DbAction.WithEntity<?> dependingOn, RelationalPersistentEntity<?> persistentEntity) {
		return persistentEntity.getIdentifierAccessor(dependingOn.getEntity()).getIdentifier();
	}

	private String getColumnNameForReverseColumn(DbAction.WithPropertyPath<?> action) {

		PersistentPropertyPath<RelationalPersistentProperty> path = action.getPropertyPath();
		return path.getRequiredLeafProperty().getReverseColumnName();
	}
}
