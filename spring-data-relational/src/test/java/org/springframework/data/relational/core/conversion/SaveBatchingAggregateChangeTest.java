/*
 * Copyright 2020-2024 the original author or authors.
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

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link SaveBatchingAggregateChange}.
 *
 * @author Chirag Tailor
 */
class SaveBatchingAggregateChangeTest {

	RelationalMappingContext context = new RelationalMappingContext();

	@Test // GH-537
	void startsWithNoActions() {

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);

		assertThat(extractActions(change)).isEmpty();
	}

	@Nested
	class RootActionsTests {
		@Test // GH-537
		void yieldsUpdateRoot() {

			Root root = new Root(1L, null);
			DbAction.UpdateRoot<Root> rootUpdate = new DbAction.UpdateRoot<>(root, null);
			RootAggregateChange<Root> aggregateChange = MutableAggregateChange.forSave(root);
			aggregateChange.setRootAction(rootUpdate);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange);

			assertThat(extractActions(change)).containsExactly(rootUpdate);
		}

		@Test // GH-537
		void yieldsSingleInsertRoot_followedByUpdateRoot_asIndividualActions() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(1L, null);
			DbAction.UpdateRoot<Root> root2Update = new DbAction.UpdateRoot<>(root2, null);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Update);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange1);
			change.add(aggregateChange2);

			assertThat(extractActions(change)) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly( //
							Tuple.tuple(DbAction.InsertRoot.class, Root.class, IdValueSource.GENERATED), //
							Tuple.tuple(DbAction.UpdateRoot.class, Root.class, IdValueSource.PROVIDED));
		}

		@Test // GH-537
		void yieldsMultipleMatchingInsertRoot_followedByUpdateRoot_asBatchInsertRootAction() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(2L, null);
			DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Insert);

			Root root3 = new Root(3L, null);
			DbAction.UpdateRoot<Root> root3Update = new DbAction.UpdateRoot<>(root3, null);
			RootAggregateChange<Root> aggregateChange3 = MutableAggregateChange.forSave(root3);
			aggregateChange3.setRootAction(root3Update);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange1);
			change.add(aggregateChange2);
			change.add(aggregateChange3);

			List<DbAction<?>> actions = extractActions(change);
			assertThat(actions) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly( //
							Tuple.tuple(DbAction.BatchInsertRoot.class, Root.class, IdValueSource.GENERATED), //
							Tuple.tuple(DbAction.UpdateRoot.class, Root.class, IdValueSource.PROVIDED));
			assertThat(getBatchWithValueAction(actions, Root.class, DbAction.BatchInsertRoot.class).getActions())
					.containsExactly(root1Insert, root2Insert);
		}

		@Test // GH-537
		void yieldsInsertRoot() {

			Root root = new Root(1L, null);
			DbAction.InsertRoot<Root> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange = MutableAggregateChange.forSave(root);
			aggregateChange.setRootAction(rootInsert);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange);

			assertThat(extractActions(change)).containsExactly(rootInsert);
		}

		@Test // GH-537
		void yieldsSingleInsertRoot_followedByNonMatchingInsertRoot_asIndividualActions() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(2L, null);
			DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.PROVIDED);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Insert);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange1);
			change.add(aggregateChange2);

			assertThat(extractActions(change)).containsExactly(root1Insert, root2Insert);
		}

		@Test // GH-537
		void yieldsMultipleMatchingInsertRoot_followedByNonMatchingInsertRoot_asBatchInsertRootAction() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(2L, null);
			DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Insert);

			Root root3 = new Root(3L, null);
			DbAction.InsertRoot<Root> root3Insert = new DbAction.InsertRoot<>(root3, IdValueSource.PROVIDED);
			RootAggregateChange<Root> aggregateChange3 = MutableAggregateChange.forSave(root3);
			aggregateChange3.setRootAction(root3Insert);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange1);
			change.add(aggregateChange2);
			change.add(aggregateChange3);

			List<DbAction<?>> actions = extractActions(change);
			assertThat(actions) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly( //
							Tuple.tuple(DbAction.BatchInsertRoot.class, Root.class, IdValueSource.GENERATED), //
							Tuple.tuple(DbAction.InsertRoot.class, Root.class, IdValueSource.PROVIDED));
			assertThat(getBatchWithValueAction(actions, Root.class, DbAction.BatchInsertRoot.class).getActions())
					.containsExactly(root1Insert, root2Insert);
		}

		@Test // GH-537
		void yieldsMultipleMatchingInsertRoot_asBatchInsertRootAction() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(2L, null);
			DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Insert);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
			change.add(aggregateChange1);
			change.add(aggregateChange2);

			List<DbAction<?>> actions = extractActions(change);
			assertThat(actions) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly(Tuple.tuple(DbAction.BatchInsertRoot.class, Root.class, IdValueSource.GENERATED));
			assertThat(getBatchWithValueAction(actions, Root.class, DbAction.BatchInsertRoot.class).getActions())
					.containsExactly(root1Insert, root2Insert);
		}

		@Test // GH-537
		void yieldsPreviouslyYieldedInsertRoot_asBatchInsertRootAction_whenAdditionalMatchingInsertRootIsAdded() {

			Root root1 = new Root(1L, null);
			DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
			aggregateChange1.setRootAction(root1Insert);

			Root root2 = new Root(2L, null);
			DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
			RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
			aggregateChange2.setRootAction(root2Insert);

			BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);

			change.add(aggregateChange1);

			assertThat(extractActions(change)) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly(Tuple.tuple(DbAction.InsertRoot.class, Root.class, IdValueSource.GENERATED));

			change.add(aggregateChange2);

			List<DbAction<?>> actions = extractActions(change);
			assertThat(actions) //
					.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
					.containsExactly(Tuple.tuple(DbAction.BatchInsertRoot.class, Root.class, IdValueSource.GENERATED));
			assertThat(getBatchWithValueAction(actions, Root.class, DbAction.BatchInsertRoot.class).getActions())
					.containsExactly(root1Insert, root2Insert);
		}
	}

	@Test // GH-537
	void yieldsRootActionsBeforeDeleteActions() {

		Root root1 = new Root(null, null);
		DbAction.UpdateRoot<Root> root1Update = new DbAction.UpdateRoot<>(root1, null);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Update);

		DbAction.Delete<?> root1IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange1.addAction(root1IntermediateDelete);

		Root root2 = new Root(null, null);
		DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		assertThat(extractActions(change)).extracting(DbAction::getClass, DbAction::getEntityType).containsExactly( //
				Tuple.tuple(DbAction.UpdateRoot.class, Root.class), //
				Tuple.tuple(DbAction.InsertRoot.class, Root.class), //
				Tuple.tuple(DbAction.Delete.class, Intermediate.class));
	}

	@Test // GH-537
	void yieldsNestedDeleteActionsInTreeOrderFromLeavesToRoot() {

		Root root1 = new Root(1L, null);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(new DbAction.UpdateRoot<>(root1, null));
		DbAction.Delete<Intermediate> root1IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange1.addAction(root1IntermediateDelete);

		Root root2 = new Root(2L, null);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(new DbAction.UpdateRoot<>(root2, null));

		DbAction.Delete<?> root2LeafDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate.leaf", Root.class));
		aggregateChange2.addAction(root2LeafDelete);

		DbAction.Delete<Intermediate> root2IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange2.addAction(root2IntermediateDelete);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions).extracting(DbAction::getClass, DbAction::getEntityType).containsSubsequence(
				Tuple.tuple(DbAction.Delete.class, Leaf.class), //
				Tuple.tuple(DbAction.BatchDelete.class, Intermediate.class));
		assertThat(getBatchWithValueAction(actions, Intermediate.class, DbAction.BatchDelete.class).getActions())
				.containsExactly(root1IntermediateDelete, root2IntermediateDelete);
	}

	@Test // GH-537
	void yieldsDeleteActionsAsBatchDeletes_groupedByPath_whenGroupContainsMultipleDeletes() {

		Root root = new Root(1L, null);
		RootAggregateChange<Root> aggregateChange = MutableAggregateChange.forSave(root);
		DbAction.UpdateRoot<Root> updateRoot = new DbAction.UpdateRoot<>(root, null);
		aggregateChange.setRootAction(updateRoot);
		DbAction.Delete<Intermediate> intermediateDelete1 = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		DbAction.Delete<Intermediate> intermediateDelete2 = new DbAction.Delete<>(2L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange.addAction(intermediateDelete1);
		aggregateChange.addAction(intermediateDelete2);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions).extracting(DbAction::getClass, DbAction::getEntityType) //
				.containsExactly( //
						Tuple.tuple(DbAction.UpdateRoot.class, Root.class), //
						Tuple.tuple(DbAction.BatchDelete.class, Intermediate.class));
		assertThat(getBatchWithValueAction(actions, Intermediate.class, DbAction.BatchDelete.class).getActions())
				.containsExactly(intermediateDelete1, intermediateDelete2);
	}

	@Test // GH-537
	void yieldsDeleteActionsBeforeInsertActions() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Insert);
		Intermediate root1Intermediate = new Intermediate(null, "root1Intermediate", null);
		DbAction.Insert<?> root1IntermediateInsert = new DbAction.Insert<>(root1Intermediate,
				context.getPersistentPropertyPath("intermediate", Root.class), root1Insert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange1.addAction(root1IntermediateInsert);

		Root root2 = new Root(1L, null);
		DbAction.UpdateRoot<Root> root2Update = new DbAction.UpdateRoot<>(root2, null);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Update);
		DbAction.Delete<?> root2IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange2.addAction(root2IntermediateDelete);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		assertThat(extractActions(change)).extracting(DbAction::getClass, DbAction::getEntityType).containsSubsequence( //
				Tuple.tuple(DbAction.Delete.class, Intermediate.class), //
				Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class));
	}

	@Test // GH-537
	void yieldsInsertActionsAsBatchInserts_groupedByIdValueSource() {

		Root root = new Root(null, null);
		DbAction.InsertRoot<Root> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange = MutableAggregateChange.forSave(root);
		aggregateChange.setRootAction(rootInsert);

		Intermediate intermediateGeneratedId = new Intermediate(null, "intermediateGeneratedId", null);
		DbAction.Insert<Intermediate> intermediateInsertGeneratedId = new DbAction.Insert<>(intermediateGeneratedId,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(intermediateInsertGeneratedId);

		Intermediate intermediateProvidedId = new Intermediate(123L, "intermediateProvidedId", null);
		DbAction.Insert<Intermediate> intermediateInsertProvidedId = new DbAction.Insert<>(intermediateProvidedId,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.PROVIDED);
		aggregateChange.addAction(intermediateInsertProvidedId);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions)
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource) //
				.containsSubsequence( //
						Tuple.tuple(DbAction.InsertRoot.class, Root.class, IdValueSource.GENERATED), //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.PROVIDED)) //
				.containsSubsequence( //
						Tuple.tuple(DbAction.InsertRoot.class, Root.class, IdValueSource.GENERATED), //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED)) //
				.doesNotContain(Tuple.tuple(DbAction.Insert.class, Intermediate.class));
		assertThat(getBatchWithValueAction(actions, Intermediate.class, DbAction.BatchInsert.class, IdValueSource.GENERATED)
				.getActions()).containsExactly(intermediateInsertGeneratedId);
		assertThat(getBatchWithValueAction(actions, Intermediate.class, DbAction.BatchInsert.class, IdValueSource.PROVIDED)
				.getActions()).containsExactly(intermediateInsertProvidedId);
	}

	@Test // GH-537
	void yieldsNestedInsertActionsInTreeOrderFromRootToLeaves() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Insert);

		Intermediate root1Intermediate = new Intermediate(null, "root1Intermediate", null);
		DbAction.Insert<Intermediate> root1IntermediateInsert = new DbAction.Insert<>(root1Intermediate,
				context.getPersistentPropertyPath("intermediate", Root.class), root1Insert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange1.addAction(root1IntermediateInsert);

		Leaf root1Leaf = new Leaf(null, "root1Leaf");
		DbAction.Insert<Leaf> root1LeafInsert = new DbAction.Insert<>(root1Leaf,
				context.getPersistentPropertyPath("intermediate.leaf", Root.class), root1IntermediateInsert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange1.addAction(root1LeafInsert);

		Root root2 = new Root(null, null);
		DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);

		Intermediate root2Intermediate = new Intermediate(null, "root2Intermediate", null);
		DbAction.Insert<Intermediate> root2IntermediateInsert = new DbAction.Insert<>(root2Intermediate,
				context.getPersistentPropertyPath("intermediate", Root.class), root2Insert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange2.addAction(root2IntermediateInsert);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions)
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
				.containsSubsequence( //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED),
						Tuple.tuple(DbAction.BatchInsert.class, Leaf.class, IdValueSource.GENERATED));
		assertThat(getBatchWithValueAction(actions, Intermediate.class, DbAction.BatchInsert.class).getActions()) //
				.containsExactly(root1IntermediateInsert, root2IntermediateInsert);
		assertThat(getBatchWithValueAction(actions, Leaf.class, DbAction.BatchInsert.class).getActions()) //
				.containsExactly(root1LeafInsert);
	}

	@Test  // GH-537
	void yieldsInsertsWithSameLengthReferences_asSeparateInserts() {

		RootWithSameLengthReferences root = new RootWithSameLengthReferences(null, null, null);
		DbAction.InsertRoot<RootWithSameLengthReferences> rootInsert = new DbAction.InsertRoot<>(root,
				IdValueSource.GENERATED);
		RootAggregateChange<RootWithSameLengthReferences> aggregateChange = MutableAggregateChange.forSave(root);
		aggregateChange.setRootAction(rootInsert);

		Intermediate one = new Intermediate(null, "one", null);
		DbAction.Insert<Intermediate> oneInsert = new DbAction.Insert<>(one,
				context.getPersistentPropertyPath("one", RootWithSameLengthReferences.class), rootInsert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange.addAction(oneInsert);

		Intermediate two = new Intermediate(null, "two", null);
		DbAction.Insert<Intermediate> twoInsert = new DbAction.Insert<>(two,
				context.getPersistentPropertyPath("two", RootWithSameLengthReferences.class), rootInsert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange.addAction(twoInsert);

		BatchingAggregateChange<RootWithSameLengthReferences, RootAggregateChange<RootWithSameLengthReferences>> change = //
				BatchingAggregateChange.forSave(RootWithSameLengthReferences.class);
		change.add(aggregateChange);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions)
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
				.containsSubsequence( //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED),
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED));
		List<DbAction.BatchWithValue<Intermediate, DbAction<Intermediate>, Object>> batchInsertActions = getBatchWithValueActions(
				actions, Intermediate.class, DbAction.BatchInsert.class);
		assertThat(batchInsertActions).hasSize(2);
		assertThat(batchInsertActions.get(0).getActions()).containsExactly(oneInsert);
		assertThat(batchInsertActions.get(1).getActions()).containsExactly(twoInsert);
	}

	private <T> List<DbAction<?>> extractActions(BatchingAggregateChange<T, RootAggregateChange<T>> change) {

		List<DbAction<?>> actions = new ArrayList<>();
		change.forEachAction(actions::add);
		return actions;
	}

	private <T, A> DbAction.BatchWithValue<T, DbAction<T>, Object> getBatchWithValueAction(List<DbAction<?>> actions,
			Class<T> entityType, Class<A> batchActionType) {

		return getBatchWithValueActions(actions, entityType, batchActionType).stream().findFirst()
				.orElseThrow(() -> new RuntimeException("No BatchWithValue action found"));
	}

	private <T, A> DbAction.BatchWithValue<T, DbAction<T>, Object> getBatchWithValueAction(List<DbAction<?>> actions,
			Class<T> entityType, Class<A> batchActionType, Object batchValue) {

		return getBatchWithValueActions(actions, entityType, batchActionType).stream()
				.filter(batchWithValue -> batchWithValue.getBatchValue() == batchValue).findFirst().orElseThrow(
						() -> new RuntimeException(String.format("No BatchWithValue with batch value '%s' found", batchValue)));
	}

	@SuppressWarnings("unchecked")
	private <T, A> List<DbAction.BatchWithValue<T, DbAction<T>, Object>> getBatchWithValueActions(
			List<DbAction<?>> actions, Class<T> entityType, Class<A> batchActionType) {

		return actions.stream() //
				.filter(dbAction -> dbAction.getClass().equals(batchActionType)) //
				.filter(dbAction -> dbAction.getEntityType().equals(entityType)) //
				.map(dbAction -> (DbAction.BatchWithValue<T, DbAction<T>, Object>) dbAction).collect(Collectors.toList());
	}

	static final class RootWithSameLengthReferences {

		@Id
		private final Long id;
		private final Intermediate one;
		private final Intermediate two;

		public RootWithSameLengthReferences(Long id, Intermediate one, Intermediate two) {
			this.id = id;
			this.one = one;
			this.two = two;
		}

		public Long getId() {
			return this.id;
		}

		public Intermediate getOne() {
			return this.one;
		}

		public Intermediate getTwo() {
			return this.two;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof RootWithSameLengthReferences)) return false;
			final RootWithSameLengthReferences other = (RootWithSameLengthReferences) o;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$one = this.getOne();
			final Object other$one = other.getOne();
			if (this$one == null ? other$one != null : !this$one.equals(other$one)) return false;
			final Object this$two = this.getTwo();
			final Object other$two = other.getTwo();
			if (this$two == null ? other$two != null : !this$two.equals(other$two)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $one = this.getOne();
			result = result * PRIME + ($one == null ? 43 : $one.hashCode());
			final Object $two = this.getTwo();
			result = result * PRIME + ($two == null ? 43 : $two.hashCode());
			return result;
		}

		public String toString() {
			return "SaveBatchingAggregateChangeTest.RootWithSameLengthReferences(id=" + this.getId() + ", one=" + this.getOne() + ", two=" + this.getTwo() + ")";
		}
	}

	static final class Root {

		@Id
		private final Long id;
		private final Intermediate intermediate;

		public Root(Long id, Intermediate intermediate) {
			this.id = id;
			this.intermediate = intermediate;
		}

		public Long getId() {
			return this.id;
		}

		public Intermediate getIntermediate() {
			return this.intermediate;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Root)) return false;
			final Root other = (Root) o;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$intermediate = this.getIntermediate();
			final Object other$intermediate = other.getIntermediate();
			if (this$intermediate == null ? other$intermediate != null : !this$intermediate.equals(other$intermediate))
				return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $intermediate = this.getIntermediate();
			result = result * PRIME + ($intermediate == null ? 43 : $intermediate.hashCode());
			return result;
		}

		public String toString() {
			return "SaveBatchingAggregateChangeTest.Root(id=" + this.getId() + ", intermediate=" + this.getIntermediate() + ")";
		}
	}

	static final class Intermediate {

		@Id
		private final Long id;
		private final String name;
		private final Leaf leaf;

		public Intermediate(Long id, String name, Leaf leaf) {
			this.id = id;
			this.name = name;
			this.leaf = leaf;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Leaf getLeaf() {
			return this.leaf;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Intermediate)) return false;
			final Intermediate other = (Intermediate) o;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
			final Object this$leaf = this.getLeaf();
			final Object other$leaf = other.getLeaf();
			if (this$leaf == null ? other$leaf != null : !this$leaf.equals(other$leaf)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $leaf = this.getLeaf();
			result = result * PRIME + ($leaf == null ? 43 : $leaf.hashCode());
			return result;
		}

		public String toString() {
			return "SaveBatchingAggregateChangeTest.Intermediate(id=" + this.getId() + ", name=" + this.getName() + ", leaf=" + this.getLeaf() + ")";
		}
	}

	static final class Leaf {

		@Id
		private final Long id;
		private final String name;

		public Leaf(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public boolean equals(final Object o) {
			if (o == this) return true;
			if (!(o instanceof Leaf)) return false;
			final Leaf other = (Leaf) o;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id)) return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (this$name == null ? other$name != null : !this$name.equals(other$name)) return false;
			return true;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			return result;
		}

		public String toString() {
			return "SaveBatchingAggregateChangeTest.Leaf(id=" + this.getId() + ", name=" + this.getName() + ")";
		}
	}
}
