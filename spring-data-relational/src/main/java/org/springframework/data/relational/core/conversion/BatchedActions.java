/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import static java.util.Collections.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Collects actions of a certain type and allows to consume them in a batched fashion, i.e. "similar" actions get
 * combined into a batched action variant.
 * 
 * @param <S> type of the s</b>ingular action.
 * @param <B> type of the <b>b</b>atched action.
 * @param <C> type of the <b>c</b>ontainer used for gathering singular actions.
 * @author Jens Schauder
 * @since 3.0
 */
class BatchedActions<S extends DbAction.WithPropertyPath, B extends DbAction.BatchWithValue, C> {

	private static final Comparator<PersistentPropertyPath<RelationalPersistentProperty>> PATH_LENGTH_COMPARATOR = //
			Comparator.comparing(PersistentPropertyPath::getLength);
	private static final Comparator<PersistentPropertyPath<RelationalPersistentProperty>> REVERSE_PATH_LENGTH_COMPARATOR = //
			PATH_LENGTH_COMPARATOR.reversed();

	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, C> actionMap = new HashMap<>();

	private final Combiner<S, C, B> combiner;
	private final Comparator<PersistentPropertyPath<RelationalPersistentProperty>> sorting;

	static BatchedActions<DbAction.Delete, DbAction.BatchDelete, List<DbAction.Delete>> batchedDeletes() {
		return new BatchedActions<>(DeleteCombiner.INSTANCE, REVERSE_PATH_LENGTH_COMPARATOR);
	}

	static BatchedActions<DbAction.Insert, DbAction.BatchInsert, Map<IdValueSource, List<DbAction.Insert>>> batchedInserts() {
		return new BatchedActions<>(InsertCombiner.INSTANCE, PATH_LENGTH_COMPARATOR);
	}

	private BatchedActions(Combiner<S, C, B> combiner,
			Comparator<PersistentPropertyPath<RelationalPersistentProperty>> sorting) {

		this.combiner = combiner;
		this.sorting = sorting;
	}

	/**
	 * Adds an action that might get combined with other actions into a batch.
	 * 
	 * @param action the action to combine with other actions.
	 */
	void add(S action) {
		combiner.merge(actionMap, action.getPropertyPath(), action);
	}

	void forEach(Consumer<? super DbAction> consumer) {

		combiner.forEach( //
				actionMap.entrySet().stream() //
						.sorted(Map.Entry.comparingByKey(sorting)), //
				consumer);
	}

	interface Combiner<S, C, M> {

		/**
		 * Merges an additional entry into the map of actions, which groups the actions by property path.
		 * 
		 * @param actionMap the map of actions into which the new action is to be merged.
		 * @param propertyPath the property map under which to add the action.
		 * @param action the action to be merged.
		 */
		void merge(Map<PersistentPropertyPath<RelationalPersistentProperty>, C> actionMap,
				PersistentPropertyPath<RelationalPersistentProperty> propertyPath, S action);

		/**
		 * Invokes the consumer for the actions in the sorted stream. Before passed to the consumer compatible actions will
		 * get combined into batched actions.
		 * 
		 * @param sorted
		 * @param consumer
		 */
		void forEach(Stream<Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, C>> sorted,
				Consumer<? super DbAction> consumer);
	}

	enum DeleteCombiner implements Combiner<DbAction.Delete, List<DbAction.Delete>, DbAction.BatchDelete> {
		INSTANCE;

		@Override
		public void merge(Map<PersistentPropertyPath<RelationalPersistentProperty>, List<DbAction.Delete>> actionMap,
				PersistentPropertyPath<RelationalPersistentProperty> propertyPath, DbAction.Delete action) {

			actionMap.merge( //
					propertyPath, //
					new ArrayList<>(singletonList(action)), //
					(actions, defaultValue) -> {
						actions.add(action);
						return actions;
					});
		}

		@Override
		public void forEach(
				Stream<Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, List<DbAction.Delete>>> sorted,
				Consumer<? super DbAction> consumer) {

			sorted.forEach((entry) -> {

				List<DbAction.Delete> actions = entry.getValue();
				if (actions.size() > 1) {
					singletonList(new DbAction.BatchDelete(actions)).forEach(consumer);
				} else {
					actions.forEach(consumer);
				}
			});
		}

	}

	enum InsertCombiner
			implements Combiner<DbAction.Insert, Map<IdValueSource, List<DbAction.Insert>>, DbAction.BatchInsert> {
		INSTANCE;

		@Override
		public void merge(
				Map<PersistentPropertyPath<RelationalPersistentProperty>, Map<IdValueSource, List<DbAction.Insert>>> actionMap,
				PersistentPropertyPath<RelationalPersistentProperty> propertyPath, DbAction.Insert action) {

			actionMap.merge( //
					propertyPath, //
					new HashMap<>(singletonMap(action.getIdValueSource(), new ArrayList<>(singletonList(action)))), //
					(map, mapDefaultValue) -> {
						map.merge(action.getIdValueSource(), new ArrayList<>(singletonList(action)),
								(actions, listDefaultValue) -> {
									actions.add(action);
									return actions;
								});
						return map;
					});
		}

		@Override
		public void forEach(
				Stream<Map.Entry<PersistentPropertyPath<RelationalPersistentProperty>, Map<IdValueSource, List<DbAction.Insert>>>> sorted,
				Consumer<? super DbAction> consumer) {

			sorted.forEach((entry) -> entry.getValue() //
					.forEach((idValueSource, inserts) -> consumer.accept(new DbAction.BatchInsert(inserts))));
		}
	}
}
