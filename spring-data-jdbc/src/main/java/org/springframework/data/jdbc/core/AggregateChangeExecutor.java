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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.conversion.Interpreter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalEntityVersionUtils;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Pair;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Executes an {@link AggregateChange} by handing the included {@link DbAction} instances to the interpreter. In a
 * second step ids generated by the {@link Interpreter} get propagated to the entities contained in the actions.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @since 1.2
 */
class AggregateChangeExecutor {

	private final Interpreter interpreter;
	private final RelationalConverter converter;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context;

	AggregateChangeExecutor(Interpreter interpreter, RelationalConverter converter) {

		this.interpreter = interpreter;
		this.converter = converter;
		this.context = converter.getMappingContext();
	}

	<T> void execute(AggregateChange<T> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();

		aggregateChange.forEachAction(action -> {
			action.executeWith(interpreter);
			actions.add(action);
		});

		T newRoot = populateIdsIfNecessary(actions);
		if (newRoot != null) {
			newRoot = populateRootVersionIfNecessary(newRoot, actions);
			aggregateChange.setEntity(newRoot);
		}
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T populateRootVersionIfNecessary(T newRoot, List<DbAction<?>> actions) {

		// Does the root entity have a version attribute?
		RelationalPersistentEntity<T> persistentEntity = (RelationalPersistentEntity<T>) context
				.getRequiredPersistentEntity(newRoot.getClass());
		if (!persistentEntity.hasVersionProperty()) {
			return newRoot;
		}

		// Find the root action
		Optional<DbAction<?>> rootAction = actions.parallelStream().filter(action -> action instanceof DbAction.WithVersion)
				.findFirst();

		if (!rootAction.isPresent()) {
			// This really should never happen.
			return newRoot;
		}
		DbAction.WithVersion<T> versionAction = (DbAction.WithVersion<T>) rootAction.get();

		return RelationalEntityVersionUtils.setVersionNumberOnEntity(newRoot,
				versionAction.getNextVersion(), persistentEntity, converter);
	}

	@Nullable
	private <T> T populateIdsIfNecessary(List<DbAction<?>> actions) {

		T newRoot = null;

		// have the actions so that the inserts on the leaves come first.
		List<DbAction<?>> reverseActions = new ArrayList<>(actions);
		Collections.reverse(reverseActions);

		AggregateChangeExecutor.StagedValues cascadingValues = new AggregateChangeExecutor.StagedValues();

		for (DbAction<?> action : reverseActions) {

			if (!(action instanceof DbAction.WithGeneratedId)) {
				continue;
			}

			DbAction.WithGeneratedId<?> withGeneratedId = (DbAction.WithGeneratedId<?>) action;
			Object generatedId = withGeneratedId.getGeneratedId();
			Object newEntity = setIdAndCascadingProperties(withGeneratedId, generatedId, cascadingValues);

			// the id property was immutable so we have to propagate changes up the tree
			if (newEntity != ((DbAction.WithGeneratedId<?>) action).getEntity()) {

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

	@SuppressWarnings("unchecked")
	private <S> Object setIdAndCascadingProperties(DbAction.WithGeneratedId<S> action, @Nullable Object generatedId,
			AggregateChangeExecutor.StagedValues cascadingValues) {

		S originalEntity = action.getEntity();

		RelationalPersistentEntity<S> persistentEntity = (RelationalPersistentEntity<S>) context
				.getRequiredPersistentEntity(action.getEntityType());
		PersistentPropertyAccessor<S> propertyAccessor = converter.getPropertyAccessor(persistentEntity, originalEntity);

		if (generatedId != null) {
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
