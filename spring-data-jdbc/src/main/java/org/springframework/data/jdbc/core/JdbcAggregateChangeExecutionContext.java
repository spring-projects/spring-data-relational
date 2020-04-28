/*
 * Copyright 2019 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcIdentifierBuilder;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.DbActionExecutionResult;
import org.springframework.data.relational.core.conversion.RelationalEntityVersionUtils;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Jens Schauder
 * @author Umut Erturk
 */
class JdbcAggregateChangeExecutionContext {

	private static final String UPDATE_FAILED = "Failed to update entity [%s]. Id [%s] not found in database.";
	private static final String UPDATE_FAILED_OPTIMISTIC_LOCKING = "Failed to update entity [%s]. The entity was updated since it was rea or it isn't in the database at all.";

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context;
	private final JdbcConverter converter;
	private final DataAccessStrategy accessStrategy;

	private final Map<DbAction<?>, DbActionExecutionResult> results = new LinkedHashMap<>();
	@Nullable private Long version;

	JdbcAggregateChangeExecutionContext(JdbcConverter converter, DataAccessStrategy accessStrategy) {

		this.converter = converter;
		this.context = converter.getMappingContext();
		this.accessStrategy = accessStrategy;
	}

	<T> void executeInsertRoot(DbAction.InsertRoot<T> insert) {
		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(insert.getEntityType());

		Object id;
		if (persistentEntity.hasVersionProperty()) {

			RelationalPersistentProperty versionProperty = persistentEntity.getVersionProperty();

			Assert.state(versionProperty != null, "Version property must not be null at this stage.");

			long initialVersion = versionProperty.getActualType().isPrimitive() ? 1L : 0;

			T rootEntity = RelationalEntityVersionUtils.setVersionNumberOnEntity( //
					insert.getEntity(), initialVersion, persistentEntity, converter);

			id = accessStrategy.insert(rootEntity, insert.getEntityType(), Identifier.empty());

			setNewVersion(initialVersion);
		} else {
			id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), Identifier.empty());
		}

		add(new DbActionExecutionResult(insert, id));
	}

	<T> void executeInsert(DbAction.Insert<T> insert) {

		Identifier parentKeys = getParentKeys(insert, converter);
		Object id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), parentKeys);
		add(new DbActionExecutionResult(insert, id));
	}

	<T> void executeUpdateRoot(DbAction.UpdateRoot<T> update) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(update.getEntityType());

		if (persistentEntity.hasVersionProperty()) {
			updateWithVersion(update, persistentEntity);
		} else {

			updateWithoutVersion(update);
		}
	}

	<T> void executeUpdate(DbAction.Update<T> update) {

		if (!accessStrategy.update(update.getEntity(), update.getEntityType())) {

			throw new IncorrectUpdateSemanticsDataAccessException(
					String.format(UPDATE_FAILED, update.getEntity(), getIdFrom(update)));
		}
	}

	<T> void executeDeleteRoot(DbAction.DeleteRoot<T> delete) {

		if (delete.getPreviousVersion() != null) {

			RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(delete.getEntityType());
			if (persistentEntity.hasVersionProperty()) {

				accessStrategy.deleteWithVersion(delete.getId(), delete.getEntityType(), delete.getPreviousVersion());
				return;
			}
		}

		accessStrategy.delete(delete.getId(), delete.getEntityType());
	}

	<T> void executeDelete(DbAction.Delete<T> delete) {

		accessStrategy.delete(delete.getRootId(), delete.getPropertyPath());
	}

	<T> void executeDeleteAllRoot(DbAction.DeleteAllRoot<T> deleteAllRoot) {

		accessStrategy.deleteAll(deleteAllRoot.getEntityType());
	}

	<T> void executeDeleteAll(DbAction.DeleteAll<T> delete) {

		accessStrategy.deleteAll(delete.getPropertyPath());
	}

	<T> void executeMerge(DbAction.Merge<T> merge) {

		// temporary implementation
		if (!accessStrategy.update(merge.getEntity(), merge.getEntityType())) {

			Object id = accessStrategy.insert(merge.getEntity(), merge.getEntityType(), getParentKeys(merge, converter));
			add(new DbActionExecutionResult(merge, id));
		} else {
			add(new DbActionExecutionResult());
		}
	}

	private void add(DbActionExecutionResult result) {
		results.put(result.getAction(), result);
	}

	private Identifier getParentKeys(DbAction.WithDependingOn<?> action, JdbcConverter converter) {

		Object id = getParentId(action);

		JdbcIdentifierBuilder identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, new PersistentPropertyPathExtension(context, action.getPropertyPath()), id);

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

		return getPotentialGeneratedIdFrom(idOwningAction);
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

	private Object getPotentialGeneratedIdFrom(DbAction.WithEntity<?> idOwningAction) {

		if (idOwningAction instanceof DbAction.WithGeneratedId) {

			Object generatedId;
			DbActionExecutionResult dbActionExecutionResult = results.get(idOwningAction);
			generatedId = dbActionExecutionResult == null ? null : dbActionExecutionResult.getId();

			if (generatedId != null) {
				return generatedId;
			}
		}

		return getIdFrom(idOwningAction);
	}

	private Object getIdFrom(DbAction.WithEntity<?> idOwningAction) {

		RelationalPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(idOwningAction.getEntityType());
		Object identifier = persistentEntity.getIdentifierAccessor(idOwningAction.getEntity()).getIdentifier();

		Assert.state(identifier != null, "Couldn't obtain a required id value");

		return identifier;
	}

	private void setNewVersion(long version) {

		Assert.isNull(this.version, "A new version was set a second time.");

		this.version = version;
	}

	private long getNewVersion() {

		Assert.notNull(version, "A new version was requested, but none was set.");

		return version;
	}

	private boolean hasNewVersion() {
		return version != null;
	}

	<T> T populateRootVersionIfNecessary(T newRoot) {

		if (!hasNewVersion()) {
			return newRoot;
		}
		// Does the root entity have a version attribute?
		RelationalPersistentEntity<T> persistentEntity = (RelationalPersistentEntity<T>) context
				.getRequiredPersistentEntity(newRoot.getClass());

		return RelationalEntityVersionUtils.setVersionNumberOnEntity(newRoot, getNewVersion(), persistentEntity, converter);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	<T> T populateIdsIfNecessary() {

		T newRoot = null;

		// have the results so that the inserts on the leaves come first.
		List<DbActionExecutionResult> reverseResults = new ArrayList<>(results.values());
		Collections.reverse(reverseResults);

		StagedValues cascadingValues = new StagedValues();

		for (DbActionExecutionResult result : reverseResults) {

			DbAction<?> action = result.getAction();

			if (!(action instanceof DbAction.WithGeneratedId)) {
				continue;
			}

			DbAction.WithEntity<?> withEntity = (DbAction.WithGeneratedId<?>) action;
			Object newEntity = setIdAndCascadingProperties(withEntity, result.getId(), cascadingValues);

			// the id property was immutable so we have to propagate changes up the tree
			if (newEntity != withEntity.getEntity()) {

				if (action instanceof DbAction.Insert) {
					DbAction.Insert<?> insert = (DbAction.Insert<?>) action;

					Pair<?, ?> qualifier = insert.getQualifier();

					cascadingValues.stage(insert.getDependingOn(), insert.getPropertyPath(),
							qualifier == null ? null : qualifier.getSecond(), newEntity);

				} else if (action instanceof DbAction.InsertRoot) {
					newRoot = (T) newEntity;
				}
			}
		}

		return newRoot;
	}

	private <S> Object setIdAndCascadingProperties(DbAction.WithEntity<S> action, @Nullable Object generatedId,
			StagedValues cascadingValues) {

		S originalEntity = action.getEntity();

		RelationalPersistentEntity<S> persistentEntity = (RelationalPersistentEntity<S>) context
				.getRequiredPersistentEntity(action.getEntityType());
		PersistentPropertyAccessor<S> propertyAccessor = converter.getPropertyAccessor(persistentEntity, originalEntity);

		if (generatedId != null && persistentEntity.hasIdProperty()) {
			propertyAccessor.setProperty(persistentEntity.getRequiredIdProperty(), generatedId);
		}

		// set values of changed immutables referenced by this entity
		cascadingValues.forEachPath(action, (persistentPropertyPath, o) -> propertyAccessor
				.setProperty(getRelativePath(action, persistentPropertyPath), o));

		return propertyAccessor.getBean();
	}

	@SuppressWarnings("unchecked")
	private PersistentPropertyPath<?> getRelativePath(DbAction<?> action, PersistentPropertyPath<?> pathToValue) {

		if (action instanceof DbAction.Insert) {
			return pathToValue.getExtensionForBaseOf(((DbAction.Insert) action).getPropertyPath());
		}

		if (action instanceof DbAction.InsertRoot) {
			return pathToValue;
		}

		throw new IllegalArgumentException(String.format("DbAction of type %s is not supported.", action.getClass()));
	}

	private <T> RelationalPersistentEntity<T> getRequiredPersistentEntity(Class<T> type) {
		return (RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(type);
	}

	private <T> void updateWithoutVersion(DbAction.UpdateRoot<T> update) {

		if (!accessStrategy.update(update.getEntity(), update.getEntityType())) {

			throw new IncorrectUpdateSemanticsDataAccessException(
					String.format(UPDATE_FAILED, update.getEntity(), getIdFrom(update)));
		}
	}

	private <T> void updateWithVersion(DbAction.UpdateRoot<T> update, RelationalPersistentEntity<T> persistentEntity) {

		// If the root aggregate has a version property, increment it.
		Number previousVersion = RelationalEntityVersionUtils.getVersionNumberFromEntity(update.getEntity(),
				persistentEntity, converter);

		Assert.notNull(previousVersion, "The root aggregate cannot be updated because the version property is null.");

		setNewVersion(previousVersion.longValue() + 1);

		T rootEntity = RelationalEntityVersionUtils.setVersionNumberOnEntity(update.getEntity(), getNewVersion(),
				persistentEntity, converter);

		if (!accessStrategy.updateWithVersion(rootEntity, update.getEntityType(), previousVersion)) {

			throw new OptimisticLockingFailureException(String.format(UPDATE_FAILED_OPTIMISTIC_LOCKING, update.getEntity()));
		}
	}

	/**
	 * Accumulates information about staged immutable objects in an aggregate that require updating because their state
	 * changed because of {@link DbAction} execution.
	 */
	private static class StagedValues {

		static final List<MultiValueAggregator> aggregators = Arrays.asList(SetAggregator.INSTANCE, MapAggregator.INSTANCE,
				ListAggregator.INSTANCE, SingleElementAggregator.INSTANCE);

		Map<DbAction, Map<PersistentPropertyPath, Object>> values = new HashMap<>();

		/**
		 * Adds a value that needs to be set in an entity higher up in the tree of entities in the aggregate. If the
		 * attribute to be set is multivalued this method expects only a single element.
		 *
		 * @param action The action responsible for persisting the entity that needs the added value set. Must not be
		 *          {@literal null}.
		 * @param path The path to the property in which to set the value. Must not be {@literal null}.
		 * @param qualifier If {@code path} is a qualified multivalued properties this parameter contains the qualifier. May
		 *          be {@literal null}.
		 * @param value The value to be set. Must not be {@literal null}.
		 */
		@SuppressWarnings("unchecked")
		<T> void stage(DbAction<?> action, PersistentPropertyPath path, @Nullable Object qualifier, Object value) {

			MultiValueAggregator<T> aggregator = getAggregatorFor(path);

			Map<PersistentPropertyPath, Object> valuesForPath = this.values.computeIfAbsent(action,
					dbAction -> new HashMap<>());

			T currentValue = (T) valuesForPath.computeIfAbsent(path,
					persistentPropertyPath -> aggregator.createEmptyInstance());

			Object newValue = aggregator.add(currentValue, qualifier, value);

			valuesForPath.put(path, newValue);
		}

		private MultiValueAggregator getAggregatorFor(PersistentPropertyPath path) {

			PersistentProperty property = path.getRequiredLeafProperty();
			for (MultiValueAggregator aggregator : aggregators) {
				if (aggregator.handles(property)) {
					return aggregator;
				}
			}

			throw new IllegalStateException(String.format("Can't handle path %s", path));
		}

		/**
		 * Performs the given action for each entry in this the staging area that are provided by {@link DbAction} until all
		 * {@link PersistentPropertyPath} have been processed or the action throws an exception. The {@link BiConsumer
		 * action} is called with each applicable {@link PersistentPropertyPath} and {@code value} that is assignable to the
		 * property.
		 */
		void forEachPath(DbAction<?> dbAction, BiConsumer<PersistentPropertyPath, Object> action) {
			values.getOrDefault(dbAction, Collections.emptyMap()).forEach(action);
		}
	}

	interface MultiValueAggregator<T> {

		default Class<? super T> handledType() {
			return Object.class;
		}

		default boolean handles(PersistentProperty property) {
			return handledType().isAssignableFrom(property.getType());
		}

		@Nullable
		T createEmptyInstance();

		T add(@Nullable T aggregate, @Nullable Object qualifier, Object value);

	}

	private enum SetAggregator implements MultiValueAggregator<Set> {

		INSTANCE;

		@Override
		public Class<Set> handledType() {
			return Set.class;
		}

		@Override
		public Set createEmptyInstance() {
			return new HashSet();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Set add(@Nullable Set set, @Nullable Object qualifier, Object value) {

			Assert.notNull(set, "Set must not be null");

			set.add(value);
			return set;
		}
	}

	private enum ListAggregator implements MultiValueAggregator<List> {

		INSTANCE;

		@Override
		public boolean handles(PersistentProperty property) {
			return property.isCollectionLike();
		}

		@Override
		public List createEmptyInstance() {
			return new ArrayList();
		}

		@SuppressWarnings("unchecked")
		@Override
		public List add(@Nullable List list, @Nullable Object qualifier, Object value) {

			Assert.notNull(list, "List must not be null.");

			int index = (int) qualifier;
			if (index >= list.size()) {
				list.add(value);
			} else {
				list.add(index, value);
			}

			return list;
		}
	}

	private enum MapAggregator implements MultiValueAggregator<Map> {

		INSTANCE;

		@Override
		public Class<Map> handledType() {
			return Map.class;
		}

		@Override
		public Map createEmptyInstance() {
			return new HashMap();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Map add(@Nullable Map map, @Nullable Object qualifier, Object value) {

			Assert.notNull(map, "Map must not be null.");

			map.put(qualifier, value);
			return map;
		}
	}

	private enum SingleElementAggregator implements MultiValueAggregator<Object> {

		INSTANCE;

		@Override
		@Nullable
		public Object createEmptyInstance() {
			return null;
		}

		@Override
		public Object add(@Nullable Object __null, @Nullable Object qualifier, Object value) {
			return value;
		}
	}
}
