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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.util.Assert;

/**
 * A {@link BatchingAggregateChange} implementation for save changes that can contain actions for any mix of insert and
 * update operations. When consumed, actions are yielded in the appropriate entity tree order with inserts carried out
 * from root to leaves and deletes in reverse. All operations that can be batched are grouped and combined to offer the
 * ability for an optimized batch operation to be used.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
public class SaveBatchingAggregateChange<T> implements BatchingAggregateChange<T, RootAggregateChange<T>> {

	private final Class<T> entityType;
	private final List<DbAction<?>> rootActions = new ArrayList<>();
	/**
	 * Holds a list of InsertRoot actions that are compatible with each other, in the sense, that they might be combined
	 * into a single batch.
	 */
	private final List<DbAction.InsertRoot<T>> insertRootBatchCandidates = new ArrayList<>();
	private final BatchedActions insertActions = BatchedActions.batchedInserts();
	private final BatchedActions deleteActions = BatchedActions.batchedDeletes();

	SaveBatchingAggregateChange(Class<T> entityType) {
		this.entityType = entityType;
	}

	@Override
	public Kind getKind() {
		return Kind.SAVE;
	}

	@Override
	public Class<T> getEntityType() {
		return entityType;
	}

	@Override
	public void forEachAction(Consumer<? super DbAction<?>> consumer) {

		Assert.notNull(consumer, "Consumer must not be null");

		rootActions.forEach(consumer);
		if (insertRootBatchCandidates.size() > 1) {
			consumer.accept(new DbAction.BatchInsertRoot<>(insertRootBatchCandidates));
		} else {
			insertRootBatchCandidates.forEach(consumer);
		}
		deleteActions.forEach(consumer);
		insertActions.forEach(consumer);
	}

	@Override
	public void add(RootAggregateChange<T> aggregateChange) {

		aggregateChange.forEachAction(action -> {

			if (action instanceof DbAction.UpdateRoot<?> rootAction) {

				combineBatchCandidatesIntoSingleBatchRootAction();
				rootActions.add(rootAction);
			} else if (action instanceof DbAction.InsertRoot<?> rootAction) {

				if (!insertRootBatchCandidates.isEmpty()
						&& !insertRootBatchCandidates.get(0).getIdValueSource().equals(rootAction.getIdValueSource())) {
					combineBatchCandidatesIntoSingleBatchRootAction();
				}
				// noinspection unchecked
				insertRootBatchCandidates.add((DbAction.InsertRoot<T>) rootAction);
			} else if (action instanceof DbAction.Insert<?> insertAction) {
				insertActions.add(insertAction);
			} else if (action instanceof DbAction.Delete<?> deleteAction) {
				deleteActions.add(deleteAction);
			}
		});
	}

	/**
	 * All actions gathered in {@link #insertRootBatchCandidates} are combined into a single root action and the list of
	 * batch candidates is emptied.
	 */
	private void combineBatchCandidatesIntoSingleBatchRootAction() {

		if (insertRootBatchCandidates.size() > 1) {
			rootActions.add(new DbAction.BatchInsertRoot<>(List.copyOf(insertRootBatchCandidates)));
		} else {
			rootActions.addAll(insertRootBatchCandidates);
		}
		insertRootBatchCandidates.clear();
	}

}
