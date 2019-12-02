/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.jdbc.core;

import lombok.RequiredArgsConstructor;

import java.util.Map;

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
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
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalEntityVersionUtils;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.util.Assert;

/**
 * {@link Interpreter} for {@link DbAction}s using a {@link DataAccessStrategy} for performing actual database
 * interactions.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Tyler Van Gorder
 */
@RequiredArgsConstructor
class DefaultJdbcInterpreter implements Interpreter {

	public static final String UPDATE_FAILED = "Failed to update entity [%s]. Id [%s] not found in database.";
	public static final String UPDATE_FAILED_OPTIMISTIC_LOCKING = "Failed to update entity [%s]. The entity was updated since it was rea or it isn't in the database at all.";
	private final RelationalConverter converter;
	private final RelationalMappingContext context;
	private final DataAccessStrategy accessStrategy;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.Insert)
	 */
	@Override
	public <T> void interpret(Insert<T> insert) {

		Object id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), getParentKeys(insert));
		insert.setGeneratedId(id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.InsertRoot)
	 */
	@Override
	public <T> void interpret(InsertRoot<T> insert) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(insert.getEntityType());

		Object id;
		if (persistentEntity.hasVersionProperty()) {

			T rootEntity = RelationalEntityVersionUtils.setVersionNumberOnEntity(insert.getEntity(), 1, persistentEntity,
					converter);
			id = accessStrategy.insert(rootEntity, insert.getEntityType(), Identifier.empty());
			insert.setNextVersion(1);
		} else {
			id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), Identifier.empty());
		}
		insert.setGeneratedId(id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.Update)
	 */
	@Override
	public <T> void interpret(Update<T> update) {

		if (!accessStrategy.update(update.getEntity(), update.getEntityType())) {

			throw new IncorrectUpdateSemanticsDataAccessException(
					String.format(UPDATE_FAILED, update.getEntity(), getIdFrom(update)));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.UpdateRoot)
	 */
	@Override
	public <T> void interpret(UpdateRoot<T> update) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(update.getEntityType());

		if (persistentEntity.hasVersionProperty()) {

			// If the root aggregate has a version property, increment it.
			Number previousVersion = RelationalEntityVersionUtils.getVersionNumberFromEntity(update.getEntity(),
					persistentEntity, converter);

			Assert.notNull(previousVersion, "The root aggregate cannot be updated because the version property is null.");

			T rootEntity = RelationalEntityVersionUtils.setVersionNumberOnEntity(update.getEntity(),
					previousVersion.longValue() + 1, persistentEntity, converter);

			if (accessStrategy.updateWithVersion(rootEntity, update.getEntityType(), previousVersion)) {
				// Successful update, set the in-memory version on the action.
				update.setNextVersion(previousVersion);
			} else {
				throw new OptimisticLockingFailureException(
						String.format(UPDATE_FAILED_OPTIMISTIC_LOCKING, update.getEntity()));
			}
		} else {

			if (!accessStrategy.update(update.getEntity(), update.getEntityType())) {

				throw new IncorrectUpdateSemanticsDataAccessException(
						String.format(UPDATE_FAILED, update.getEntity(), getIdFrom(update)));
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.Merge)
	 */
	@Override
	public <T> void interpret(Merge<T> merge) {

		// temporary implementation
		if (!accessStrategy.update(merge.getEntity(), merge.getEntityType())) {
			accessStrategy.insert(merge.getEntity(), merge.getEntityType(), getParentKeys(merge));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.Delete)
	 */
	@Override
	public <T> void interpret(Delete<T> delete) {
		accessStrategy.delete(delete.getRootId(), delete.getPropertyPath());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.DeleteRoot)
	 */
	@Override
	public <T> void interpret(DeleteRoot<T> delete) {

		if (delete.getPreviousVersion() != null) {

			RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(delete.getEntityType());
			if (persistentEntity.hasVersionProperty()) {

				accessStrategy.deleteWithVersion(delete.getId(), delete.getEntityType(), delete.getPreviousVersion());
				return;
			}
		}

		accessStrategy.delete(delete.getId(), delete.getEntityType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.DeleteAll)
	 */
	@Override
	public <T> void interpret(DeleteAll<T> delete) {
		accessStrategy.deleteAll(delete.getPropertyPath());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.conversion.Interpreter#interpret(org.springframework.data.relational.core.conversion.DbAction.DeleteAllRoot)
	 */
	@Override
	public <T> void interpret(DeleteAllRoot<T> deleteAllRoot) {
		accessStrategy.deleteAll(deleteAllRoot.getEntityType());
	}

	private Identifier getParentKeys(DbAction.WithDependingOn<?> action) {

		Object id = getParentId(action);

		JdbcIdentifierBuilder identifier = JdbcIdentifierBuilder //
				.forBackReferences(new PersistentPropertyPathExtension(context, action.getPropertyPath()), id);

		for (Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifier : action.getQualifiers()
				.entrySet()) {
			identifier = identifier.withQualifier(new PersistentPropertyPathExtension(context, qualifier.getKey()),
					qualifier.getValue());
		}

		return identifier.build();
	}

	private Object getParentId(DbAction.WithDependingOn<?> action) {

		PersistentPropertyPathExtension path = new PersistentPropertyPathExtension(context, action.getPropertyPath());
		PersistentPropertyPathExtension idPath = path.getIdDefiningParentPath();

		DbAction.WithEntity<?> idOwningAction = getIdOwningAction(action, idPath);

		return getIdFrom(idOwningAction);
	}

	private DbAction.WithEntity<?> getIdOwningAction(DbAction.WithEntity<?> action,
			PersistentPropertyPathExtension idPath) {

		if (!(action instanceof DbAction.WithDependingOn)) {

			Assert.state(idPath.getLength() == 0,
					"When the id path is not empty the id providing action should be of type WithDependingOn");

			return action;
		}

		DbAction.WithDependingOn<?> withDependingOn = (DbAction.WithDependingOn<?>) action;

		if (idPath.matches(withDependingOn.getPropertyPath())) {
			return action;
		}

		return getIdOwningAction(withDependingOn.getDependingOn(), idPath);
	}

	private Object getIdFrom(DbAction.WithEntity<?> idOwningAction) {

		if (idOwningAction instanceof DbAction.WithGeneratedId) {

			Object generatedId = ((DbAction.WithGeneratedId<?>) idOwningAction).getGeneratedId();

			if (generatedId != null) {
				return generatedId;
			}
		}

		RelationalPersistentEntity<?> persistentEntity = context
				.getRequiredPersistentEntity(idOwningAction.getEntityType());
		Object identifier = persistentEntity.getIdentifierAccessor(idOwningAction.getEntity()).getIdentifier();

		Assert.state(identifier != null, "Couldn't obtain a required id value");

		return identifier;
	}

	@SuppressWarnings("unchecked")
	private <T> RelationalPersistentEntity<T> getRequiredPersistentEntity(Class<T> type) {
		return (RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(type);
	}

}
