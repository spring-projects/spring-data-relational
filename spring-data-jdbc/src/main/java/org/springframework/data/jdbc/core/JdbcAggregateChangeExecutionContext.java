/*
 * Copyright 2019-2024 the original author or authors.
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

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.InsertSubject;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcIdentifierBuilder;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.DbActionExecutionResult;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A container for the data required and produced by an aggregate change execution. Most importantly it holds the
 * results of the various actions performed.
 *
 * @author Jens Schauder
 * @author Umut Erturk
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @author Mark Paluch
 */
@SuppressWarnings("rawtypes")
class JdbcAggregateChangeExecutionContext {

	private static final String UPDATE_FAILED = "Failed to update entity [%s]; Id [%s] not found in database";
	private static final String UPDATE_FAILED_OPTIMISTIC_LOCKING = "Failed to update entity [%s]; The entity was updated since it was rea or it isn't in the database at all";

	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final DataAccessStrategy accessStrategy;

	private final Map<DbAction<?>, DbActionExecutionResult> results = new LinkedHashMap<>();

	JdbcAggregateChangeExecutionContext(JdbcConverter converter, DataAccessStrategy accessStrategy) {

		this.converter = converter;
		this.context = converter.getMappingContext();
		this.accessStrategy = accessStrategy;
	}

	<T> void executeInsertRoot(DbAction.InsertRoot<T> insert) {

		Object id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), Identifier.empty(),
				insert.getIdValueSource());
		add(new DbActionExecutionResult(insert, id));
	}

	<T> void executeBatchInsertRoot(DbAction.BatchInsertRoot<T> batchInsertRoot) {

		List<DbAction.InsertRoot<T>> inserts = batchInsertRoot.getActions();
		List<InsertSubject<T>> insertSubjects = inserts.stream()
				.map(insert -> InsertSubject.describedBy(insert.getEntity(), Identifier.empty())).collect(Collectors.toList());

		Object[] ids = accessStrategy.insert(insertSubjects, batchInsertRoot.getEntityType(),
				batchInsertRoot.getBatchValue());

		for (int i = 0; i < inserts.size(); i++) {
			add(new DbActionExecutionResult(inserts.get(i), ids.length > 0 ? ids[i] : null));
		}
	}

	<T> void executeInsert(DbAction.Insert<T> insert) {

		Identifier parentKeys = getParentKeys(insert, converter);
		Object id = accessStrategy.insert(insert.getEntity(), insert.getEntityType(), parentKeys,
				insert.getIdValueSource());
		add(new DbActionExecutionResult(insert, id));
	}

	<T> void executeBatchInsert(DbAction.BatchInsert<T> batchInsert) {

		List<DbAction.Insert<T>> inserts = batchInsert.getActions();
		List<InsertSubject<T>> insertSubjects = inserts.stream()
				.map(insert -> InsertSubject.describedBy(insert.getEntity(), getParentKeys(insert, converter)))
				.collect(Collectors.toList());

		Object[] ids = accessStrategy.insert(insertSubjects, batchInsert.getEntityType(), batchInsert.getBatchValue());

		for (int i = 0; i < inserts.size(); i++) {
			add(new DbActionExecutionResult(inserts.get(i), ids.length > 0 ? ids[i] : null));
		}
	}

	<T> void executeUpdateRoot(DbAction.UpdateRoot<T> update) {

		if (update.getPreviousVersion() != null) {
			updateWithVersion(update);
		} else {
			updateWithoutVersion(update);
		}
		add(new DbActionExecutionResult(update));
	}

	<T> void executeDeleteRoot(DbAction.DeleteRoot<T> delete) {

		if (delete.getPreviousVersion() != null) {
			accessStrategy.deleteWithVersion(delete.getId(), delete.getEntityType(), delete.getPreviousVersion());
		} else {
			accessStrategy.delete(delete.getId(), delete.getEntityType());
		}
	}

	<T> void executeBatchDeleteRoot(DbAction.BatchDeleteRoot<T> batchDelete) {

		List<Object> rootIds = batchDelete.getActions().stream().map(DbAction.DeleteRoot::getId).toList();
		accessStrategy.delete(rootIds, batchDelete.getEntityType());
	}

	<T> void executeDelete(DbAction.Delete<T> delete) {

		accessStrategy.delete(delete.getRootId(), delete.getPropertyPath());
	}

	<T> void executeBatchDelete(DbAction.BatchDelete<T> batchDelete) {

		List<Object> rootIds = batchDelete.getActions().stream().map(DbAction.Delete::getRootId).toList();
		accessStrategy.delete(rootIds, batchDelete.getBatchValue());
	}

	<T> void executeDeleteAllRoot(DbAction.DeleteAllRoot<T> deleteAllRoot) {

		accessStrategy.deleteAll(deleteAllRoot.getEntityType());
	}

	<T> void executeDeleteAll(DbAction.DeleteAll<T> delete) {

		accessStrategy.deleteAll(delete.getPropertyPath());
	}

	<T> void executeAcquireLock(DbAction.AcquireLockRoot<T> acquireLock) {
		accessStrategy.acquireLockById(acquireLock.getId(), LockMode.PESSIMISTIC_WRITE, acquireLock.getEntityType());
	}

	<T> void executeAcquireLockAllRoot(DbAction.AcquireLockAllRoot<T> acquireLock) {
		accessStrategy.acquireLockAll(LockMode.PESSIMISTIC_WRITE, acquireLock.getEntityType());
	}

	private void add(DbActionExecutionResult result) {
		results.put(result.getAction(), result);
	}

	private Identifier getParentKeys(DbAction.WithDependingOn<?> action, JdbcConverter converter) {

		Object id = getParentId(action);

		JdbcIdentifierBuilder identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, context.getAggregatePath(action.getPropertyPath()), id);

		for (Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Object> qualifier : action.getQualifiers()
				.entrySet()) {
			identifier = identifier.withQualifier(context.getAggregatePath(qualifier.getKey()), qualifier.getValue());
		}

		return identifier.build();
	}

	private Object getParentId(DbAction.WithDependingOn<?> action) {

		DbAction.WithEntity<?> idOwningAction = getIdOwningAction(action,
				context.getAggregatePath(action.getPropertyPath()).getIdDefiningParentPath());

		return getPotentialGeneratedIdFrom(idOwningAction);
	}

	private DbAction.WithEntity<?> getIdOwningAction(DbAction.WithEntity<?> action, AggregatePath idPath) {

		if (!(action instanceof DbAction.WithDependingOn<?> withDependingOn)) {

			Assert.state(idPath.isRoot(),
					"When the id path is not empty the id providing action should be of type WithDependingOn");

			return action;
		}

		if (idPath.equals(context.getAggregatePath(withDependingOn.getPropertyPath()))) {
			return action;
		}

		return getIdOwningAction(withDependingOn.getDependingOn(), idPath);
	}

	private Object getPotentialGeneratedIdFrom(DbAction.WithEntity<?> idOwningAction) {

		if (IdValueSource.GENERATED.equals(idOwningAction.getIdValueSource())) {

			DbActionExecutionResult dbActionExecutionResult = results.get(idOwningAction);
			Object generatedId = Optional.ofNullable(dbActionExecutionResult) //
					.map(DbActionExecutionResult::getGeneratedId) //
					.orElse(null);

			if (generatedId != null) {
				return generatedId;
			}
		}

		return getIdFrom(idOwningAction);
	}

	private Object getIdFrom(DbAction.WithEntity<?> idOwningAction) {

		RelationalPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(idOwningAction.getEntityType());
		Object identifier = persistentEntity.getIdentifierAccessor(idOwningAction.getEntity()).getIdentifier();

		Assert.state(identifier != null, () -> "Couldn't obtain a required id value for " + persistentEntity);

		return identifier;
	}

	<T> List<T> populateIdsIfNecessary() {

		// have the results so that the inserts on the leaves come first.
		List<DbActionExecutionResult> reverseResults = new ArrayList<>(results.values());
		Collections.reverse(reverseResults);

		StagedValues cascadingValues = new StagedValues();

		List<T> roots = new ArrayList<>(reverseResults.size());

		for (DbActionExecutionResult result : reverseResults) {

			DbAction.WithEntity<?> action = result.getAction();

			Object newEntity = setIdAndCascadingProperties(action, result.getGeneratedId(), cascadingValues);

			if (action instanceof DbAction.InsertRoot || action instanceof DbAction.UpdateRoot) {
				// noinspection unchecked
				roots.add((T) newEntity);
			}

			// the id property was immutable, so we have to propagate changes up the tree
			if (action instanceof DbAction.Insert<?> insert) {

				Pair<?, ?> qualifier = insert.getQualifier();
				Object qualifierValue = qualifier == null ? null : qualifier.getSecond();

				if (newEntity != action.getEntity()) {

					cascadingValues.stage(insert.getDependingOn(), insert.getPropertyPath(),
							qualifierValue, newEntity);
				} else if (insert.getPropertyPath().getLeafProperty().isCollectionLike()) {

					cascadingValues.gather(insert.getDependingOn(), insert.getPropertyPath(),
							qualifierValue, newEntity);
				}
			}
		}

		if (roots.isEmpty()) {
			throw new IllegalStateException(
					String.format("Cannot retrieve the resulting instance(s) unless a %s or %s action was successfully executed",
							DbAction.InsertRoot.class.getName(), DbAction.UpdateRoot.class.getName()));
		}

		Collections.reverse(roots);

		return roots;
	}

	@SuppressWarnings("unchecked")
	private <S> Object setIdAndCascadingProperties(DbAction.WithEntity<S> action, @Nullable Object generatedId,
			StagedValues cascadingValues) {

		S originalEntity = action.getEntity();

		RelationalPersistentEntity<S> persistentEntity = (RelationalPersistentEntity<S>) context
				.getRequiredPersistentEntity(action.getEntityType());
		PersistentPropertyPathAccessor<S> propertyAccessor = converter.getPropertyAccessor(persistentEntity,
				originalEntity);

		if (IdValueSource.GENERATED.equals(action.getIdValueSource())) {
			propertyAccessor.setProperty(persistentEntity.getRequiredIdProperty(), generatedId);
		}

		// set values of changed immutables referenced by this entity
		cascadingValues.forEachPath(action, (persistentPropertyPath, o) -> propertyAccessor
				.setProperty(getRelativePath(action, persistentPropertyPath), o));

		return propertyAccessor.getBean();
	}

	@SuppressWarnings("unchecked")
	private PersistentPropertyPath<?> getRelativePath(DbAction<?> action, PersistentPropertyPath<?> pathToValue) {

		if (action instanceof DbAction.Insert insert) {
			return pathToValue.getExtensionForBaseOf(insert.getPropertyPath());
		}

		if (action instanceof DbAction.InsertRoot) {
			return pathToValue;
		}

		if (action instanceof DbAction.UpdateRoot) {
			return pathToValue;
		}

		throw new IllegalArgumentException(String.format("DbAction of type %s is not supported", action.getClass()));
	}

	@SuppressWarnings("unchecked")
	private <T> RelationalPersistentEntity<T> getRequiredPersistentEntity(Class<T> type) {
		return (RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(type);
	}

	private <T> void updateWithoutVersion(DbAction.UpdateRoot<T> update) {

		if (!accessStrategy.update(update.getEntity(), update.getEntityType())) {

			throw new IncorrectUpdateSemanticsDataAccessException(
					String.format(UPDATE_FAILED, update.getEntity(), getIdFrom(update)));
		}
	}

	private <T> void updateWithVersion(DbAction.UpdateRoot<T> update) {

		Number previousVersion = update.getPreviousVersion();
		Assert.notNull(previousVersion, "The root aggregate cannot be updated because the version property is null");

		if (!accessStrategy.updateWithVersion(update.getEntity(), update.getEntityType(), previousVersion)) {

			throw new OptimisticLockingFailureException(String.format(UPDATE_FAILED_OPTIMISTIC_LOCKING, update.getEntity()));
		}
	}

	/**
	 * Accumulates information about staged immutable objects in an aggregate that require updating because their state
	 * changed because of {@link DbAction} execution.
	 */
	private static class StagedValues {

		static final List<MultiValueAggregator<?>> aggregators = Arrays.asList(SetAggregator.INSTANCE, MapAggregator.INSTANCE,
				ListAggregator.INSTANCE, SingleElementAggregator.INSTANCE);

		Map<DbAction, Map<PersistentPropertyPath, StagedValue>> values = new HashMap<>();

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
		void stage(DbAction<?> action, PersistentPropertyPath path, @Nullable Object qualifier, Object value) {

			StagedValue gather = gather(action, path, qualifier, value);
			gather.isStaged = true;
		}

		@SuppressWarnings("unchecked")
		<T> StagedValue gather(DbAction<?> action, PersistentPropertyPath path, @Nullable Object qualifier, Object value) {

			MultiValueAggregator<T> aggregator = getAggregatorFor(path);

			Map<PersistentPropertyPath, StagedValue> valuesForPath = this.values.computeIfAbsent(action,
					dbAction -> new HashMap<>());

			StagedValue stagedValue = valuesForPath.computeIfAbsent(path,
					persistentPropertyPath -> new StagedValue(aggregator.createEmptyInstance()));
			T currentValue = (T) stagedValue.value;

			stagedValue.value = aggregator.add(currentValue, qualifier, value);

			valuesForPath.put(path, stagedValue);

			return stagedValue;
		}

		@SuppressWarnings("unchecked")
		private <T> MultiValueAggregator<T> getAggregatorFor(PersistentPropertyPath path) {

			PersistentProperty property = path.getLeafProperty();
			for (MultiValueAggregator<?> aggregator : aggregators) {
				if (aggregator.handles(property)) {
					return (MultiValueAggregator<T>) aggregator;
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
			values.getOrDefault(dbAction, Collections.emptyMap()).forEach((persistentPropertyPath, stagedValue) -> {
				if (stagedValue.isStaged) {
					action.accept(persistentPropertyPath, stagedValue.value);
				}
			});
		}

	}

	private static class StagedValue {
		@Nullable Object value;
		boolean isStaged;

		public StagedValue(@Nullable Object value) {
			this.value = value;
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

			Assert.notNull(list, "List must not be null");
			Assert.notNull(qualifier, "ListAggregator can't handle a null qualifier");

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

			Assert.notNull(map, "Map must not be null");

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
