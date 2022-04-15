/*
 * Copyright 2020-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

import lombok.Value;

/**
 * Unit tests for {@link SaveBatchingAggregateChange}.
 *
 * @author Chirag Tailor
 */
class SaveBatchingAggregateChangeTest {

	RelationalMappingContext context = new RelationalMappingContext();

	@Test
	void startsWithNoActions() {

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);

		assertThat(extractActions(change)).isEmpty();
	}

	@Test
	void yieldsRootActions() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Insert);
		Root root2 = new Root(null, null);
		DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		assertThat(extractActions(change)).containsExactly(root1Insert, root2Insert);
	}

	@Test
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

	@Test
	void yieldsNestedDeleteActionsInTreeOrderFromLeavesToRoot() {

		Root root1 = new Root(1L, null);
		RootAggregateChange<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(new DbAction.UpdateRoot<>(root1, null));
		DbAction.Delete<?> root1IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange1.addAction(root1IntermediateDelete);

		Root root2 = new Root(1L, null);
		RootAggregateChange<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(new DbAction.UpdateRoot<>(root2, null));
		DbAction.Delete<?> root2LeafDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate.leaf", Root.class));
		aggregateChange2.addAction(root2LeafDelete);
		DbAction.Delete<?> root2IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange2.addAction(root2IntermediateDelete);

		BatchingAggregateChange<Root, RootAggregateChange<Root>> change = BatchingAggregateChange.forSave(Root.class);
		change.add(aggregateChange1);
		change.add(aggregateChange2);

		assertThat(extractActions(change)).containsSubsequence(root2LeafDelete, root1IntermediateDelete,
				root2IntermediateDelete);
	}

	@Test
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

		assertThat(extractActions(change)).containsSubsequence(root2IntermediateDelete, root1IntermediateInsert);
	}

	@Test
	void yieldsInsertActionsAsBatchInserts_groupedByIdValueSource_whenGroupContainsMultipleInserts() {

		Root root = new Root(null, null);
		DbAction.InsertRoot<Root> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		RootAggregateChange<Root> aggregateChange = MutableAggregateChange.forSave(root);
		aggregateChange.setRootAction(rootInsert);
		Intermediate intermediateGeneratedId1 = new Intermediate(null, "intermediateGeneratedId1", null);
		DbAction.Insert<Intermediate> intermediateInsertGeneratedId1 = new DbAction.Insert<>(intermediateGeneratedId1,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(intermediateInsertGeneratedId1);
		Intermediate intermediateGeneratedId2 = new Intermediate(null, "intermediateGeneratedId2", null);
		DbAction.Insert<Intermediate> intermediateInsertGeneratedId2 = new DbAction.Insert<>(intermediateGeneratedId2,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(intermediateInsertGeneratedId2);
		Intermediate intermediateProvidedId1 = new Intermediate(123L, "intermediateProvidedId1", null);
		DbAction.Insert<Intermediate> intermediateInsertProvidedId1 = new DbAction.Insert<>(intermediateProvidedId1,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.PROVIDED);
		aggregateChange.addAction(intermediateInsertProvidedId1);
		Intermediate intermediateProvidedId2 = new Intermediate(456L, "intermediateProvidedId2", null);
		DbAction.Insert<Intermediate> intermediateInsertProvidedId2 = new DbAction.Insert<>(intermediateProvidedId2,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.PROVIDED);
		aggregateChange.addAction(intermediateInsertProvidedId2);

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
		assertThat(getBatchInsertAction(actions, Intermediate.class, IdValueSource.GENERATED).getActions())
				.containsExactly(intermediateInsertGeneratedId1, intermediateInsertGeneratedId2);
		assertThat(getBatchInsertAction(actions, Intermediate.class, IdValueSource.PROVIDED).getActions())
				.containsExactly(intermediateInsertProvidedId1, intermediateInsertProvidedId2);
	}

	@Test
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
						Tuple.tuple(DbAction.Insert.class, Leaf.class, IdValueSource.GENERATED));
		assertThat(getBatchInsertAction(actions, Intermediate.class).getActions()) //
				.containsExactly(root1IntermediateInsert, root2IntermediateInsert);
	}

	@Test
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
		assertThat(actions).containsSubsequence(oneInsert, twoInsert);
	}

	private <T> DbAction.BatchInsert<T> getBatchInsertAction(List<DbAction<?>> actions, Class<T> entityType,
			IdValueSource idValueSource) {
		return getBatchInsertActions(actions, entityType).stream()
				.filter(batchInsert -> batchInsert.getBatchValue() == idValueSource).findFirst().orElseThrow(
						() -> new RuntimeException(String.format("No BatchInsert with batch value '%s' found!", idValueSource)));
	}

	private <T> DbAction.BatchInsert<T> getBatchInsertAction(List<DbAction<?>> actions, Class<T> entityType) {
		return getBatchInsertActions(actions, entityType).stream().findFirst()
				.orElseThrow(() -> new RuntimeException("No BatchInsert action found!"));
	}

	@SuppressWarnings("unchecked")
	private <T> List<DbAction.BatchInsert<T>> getBatchInsertActions(List<DbAction<?>> actions, Class<T> entityType) {

		return actions.stream() //
				.filter(dbAction -> dbAction instanceof DbAction.BatchInsert) //
				.filter(dbAction -> dbAction.getEntityType().equals(entityType)) //
				.map(dbAction -> (DbAction.BatchInsert<T>) dbAction).collect(Collectors.toList());
	}

	private <T> List<DbAction<?>> extractActions(BatchingAggregateChange<T, RootAggregateChange<T>> change) {

		List<DbAction<?>> actions = new ArrayList<>();
		change.forEachAction(actions::add);
		return actions;
	}

	@Value
	static class RootWithSameLengthReferences {
		@Id Long id;
		Intermediate one;
		Intermediate two;
	}

	@Value
	static class Root {
		@Id Long id;
		Intermediate intermediate;
	}

	@Value
	static class Intermediate {
		@Id Long id;
		String name;
		Leaf leaf;
	}

	@Value
	static class Leaf {
		@Id Long id;
		String name;
	}
}
