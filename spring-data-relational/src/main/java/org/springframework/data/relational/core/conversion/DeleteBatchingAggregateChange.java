package org.springframework.data.relational.core.conversion;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A {@link BatchingAggregateChange} implementation for delete changes that can contain actions for one or more delete
 * operations. When consumed, actions are yielded in the appropriate entity tree order with deletes carried out from
 * leaves to root. All operations that can be batched are grouped and combined to offer the ability for an optimized
 * batch operation to be used.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
public class DeleteBatchingAggregateChange<T> implements BatchingAggregateChange<T, DeleteAggregateChange<T>> {

	private final Class<T> entityType;
	private final List<DbAction.DeleteRoot<T>> rootActionsWithoutVersion = new ArrayList<>();
	private final List<DbAction.DeleteRoot<T>> rootActionsWithVersion = new ArrayList<>();
	private final List<DbAction.AcquireLockRoot<?>> lockActions = new ArrayList<>();
	private final BatchedActions deleteActions = BatchedActions.batchedDeletes();

	DeleteBatchingAggregateChange(Class<T> entityType) {
		this.entityType = entityType;
	}

	@Override
	public Kind getKind() {
		return Kind.DELETE;
	}

	@Override
	public Class<T> getEntityType() {
		return entityType;
	}

	@Override
	public void forEachAction(Consumer<? super DbAction<?>> consumer) {

		lockActions.forEach(consumer);
		deleteActions.forEach(consumer);
		if (rootActionsWithoutVersion.size() > 1) {
			consumer.accept(new DbAction.BatchDeleteRoot<>(rootActionsWithoutVersion));
		} else {
			rootActionsWithoutVersion.forEach(consumer);
		}
		rootActionsWithVersion.forEach(consumer);
	}

	@Override
	public void add(DeleteAggregateChange<T> aggregateChange) {

		aggregateChange.forEachAction(action -> {
			if (action instanceof DbAction.DeleteRoot<?> deleteRootAction) {
				// noinspection unchecked
				addDeleteRoot((DbAction.DeleteRoot<T>) deleteRootAction);
			} else if (action instanceof DbAction.Delete<?> deleteAction) {
				deleteActions.add(deleteAction);
			} else if (action instanceof DbAction.AcquireLockRoot<?> lockRootAction) {
				lockActions.add(lockRootAction);
			}
		});
	}

	private void addDeleteRoot(DbAction.DeleteRoot<T> action) {

		if (action.getPreviousVersion() == null) {
			rootActionsWithoutVersion.add(action);
		} else {
			rootActionsWithVersion.add(action);
		}
	}
}
