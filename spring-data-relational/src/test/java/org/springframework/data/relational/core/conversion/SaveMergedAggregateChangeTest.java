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

class SaveMergedAggregateChangeTest {

	RelationalMappingContext context = new RelationalMappingContext();

	@Test
	void startsWithNoActions() {

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = MutableAggregateChange.mergedSave(Root.class);

		assertThat(extractActions(change)).isEmpty();
	}

	@Test
	void yieldsRootActions() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Insert);
		Root root2 = new Root(null, null);
		DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange1) //
						.merge(aggregateChange2);

		assertThat(extractActions(change)).containsExactly(root1Insert, root2Insert);
	}

	@Test
	void yieldsRootActionsBeforeDeleteActions() {

		Root root1 = new Root(null, null);
		DbAction.UpdateRoot<Root> root1Update = new DbAction.UpdateRoot<>(root1, null);
		AggregateChangeWithRoot<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Update);
		DbAction.Delete<?> root1IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange1.addAction(root1IntermediateDelete);
		Root root2 = new Root(null, null);
		DbAction.InsertRoot<Root> root2Insert = new DbAction.InsertRoot<>(root2, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange1) //
						.merge(aggregateChange2);

		assertThat(extractActions(change)).extracting(DbAction::getClass, DbAction::getEntityType).containsExactly( //
				Tuple.tuple(DbAction.UpdateRoot.class, Root.class), //
				Tuple.tuple(DbAction.InsertRoot.class, Root.class), //
				Tuple.tuple(DbAction.Delete.class, Intermediate.class));
	}

	@Test
	void yieldsNestedDeleteActionsInTreeOrderFromLeavesToRoot() {

		Root root1 = new Root(1L, null);
		AggregateChangeWithRoot<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(new DbAction.UpdateRoot<>(root1, null));
		DbAction.Delete<?> root1IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange1.addAction(root1IntermediateDelete);

		Root root2 = new Root(1L, null);
		AggregateChangeWithRoot<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(new DbAction.UpdateRoot<>(root2, null));
		DbAction.Delete<?> root2LeafDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate.leaf", Root.class));
		aggregateChange2.addAction(root2LeafDelete);
		DbAction.Delete<?> root2IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange2.addAction(root2IntermediateDelete);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange1) //
						.merge(aggregateChange2);

		assertThat(extractActions(change)).containsSubsequence(root2LeafDelete, root1IntermediateDelete,
				root2IntermediateDelete);
	}

	@Test
	void yieldsDeleteActionsBeforeInsertActions() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
		aggregateChange1.setRootAction(root1Insert);
		Intermediate root1Intermediate = new Intermediate(null, "root1Intermediate", null);
		DbAction.Insert<?> root1IntermediateInsert = new DbAction.Insert<>(root1Intermediate,
				context.getPersistentPropertyPath("intermediate", Root.class), root1Insert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange1.addAction(root1IntermediateInsert);

		Root root2 = new Root(1L, null);
		DbAction.UpdateRoot<Root> root2Update = new DbAction.UpdateRoot<>(root2, null);
		AggregateChangeWithRoot<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Update);
		DbAction.Delete<?> root2IntermediateDelete = new DbAction.Delete<>(1L,
				context.getPersistentPropertyPath("intermediate", Root.class));
		aggregateChange2.addAction(root2IntermediateDelete);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange1) //
						.merge(aggregateChange2);

		assertThat(extractActions(change)).extracting(DbAction::getClass, DbAction::getEntityType).containsSubsequence( //
				Tuple.tuple(DbAction.Delete.class, Intermediate.class), //
				Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class));
	}

	@Test
	void yieldsInsertActionsAsBatchInserts_groupedByIdValueSource() {

		Root root = new Root(null, null);
		DbAction.InsertRoot<Root> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange = MutableAggregateChange.forSave(root);
		aggregateChange.setRootAction(rootInsert);
		Intermediate intermediateGeneratedId = new Intermediate(null, "intermediateGeneratedId", null);
		DbAction.Insert<Intermediate> intermediateInsertGeneratedId = new DbAction.Insert<>(intermediateGeneratedId,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(intermediateInsertGeneratedId);
		Intermediate intermediateProvidedId = new Intermediate(123L, "intermediateProvidedId", null);
		DbAction.Insert<Intermediate> intermediateInsertProvidedId = new DbAction.Insert<>(intermediateProvidedId,
				context.getPersistentPropertyPath("intermediate", Root.class), rootInsert, emptyMap(), IdValueSource.PROVIDED);
		aggregateChange.addAction(intermediateInsertProvidedId);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange);

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
				.containsExactly(intermediateInsertGeneratedId);
		assertThat(getBatchInsertAction(actions, Intermediate.class, IdValueSource.PROVIDED).getActions())
				.containsExactly(intermediateInsertProvidedId);
	}

	@Test
	void yieldsNestedInsertActionsInTreeOrderFromRootToLeaves() {

		Root root1 = new Root(null, null);
		DbAction.InsertRoot<Root> root1Insert = new DbAction.InsertRoot<>(root1, IdValueSource.GENERATED);
		AggregateChangeWithRoot<Root> aggregateChange1 = MutableAggregateChange.forSave(root1);
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
		AggregateChangeWithRoot<Root> aggregateChange2 = MutableAggregateChange.forSave(root2);
		aggregateChange2.setRootAction(root2Insert);
		Intermediate root2Intermediate = new Intermediate(null, "root2Intermediate", null);
		DbAction.Insert<Intermediate> root2IntermediateInsert = new DbAction.Insert<>(root2Intermediate,
				context.getPersistentPropertyPath("intermediate", Root.class), root2Insert, emptyMap(),
				IdValueSource.GENERATED);
		aggregateChange2.addAction(root2IntermediateInsert);

		MergedAggregateChange<Root, AggregateChangeWithRoot<Root>> change = //
				MutableAggregateChange.mergedSave(Root.class) //
						.merge(aggregateChange1) //
						.merge(aggregateChange2);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions)
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
				.containsSubsequence( //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED),
						Tuple.tuple(DbAction.BatchInsert.class, Leaf.class, IdValueSource.GENERATED));
		assertThat(getBatchInsertAction(actions, Intermediate.class).getActions()) //
				.containsExactly(root1IntermediateInsert, root2IntermediateInsert);
		assertThat(getBatchInsertAction(actions, Leaf.class).getActions()) //
				.containsExactly(root1LeafInsert);
	}

	@Test
	void yieldsInsertsWithSameLengthReferences_asSeparateInserts() {

		RootWithSameLengthReferences root = new RootWithSameLengthReferences(null, null, null);
		DbAction.InsertRoot<RootWithSameLengthReferences> rootInsert = new DbAction.InsertRoot<>(root, IdValueSource.GENERATED);
		AggregateChangeWithRoot<RootWithSameLengthReferences> aggregateChange = MutableAggregateChange.forSave(root);
		aggregateChange.setRootAction(rootInsert);
		Intermediate one = new Intermediate(null, "one", null);
		DbAction.Insert<Intermediate> oneInsert = new DbAction.Insert<>(one,
				context.getPersistentPropertyPath("one", RootWithSameLengthReferences.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(oneInsert);
		Intermediate two = new Intermediate(null, "two", null);
		DbAction.Insert<Intermediate> twoInsert = new DbAction.Insert<>(two,
				context.getPersistentPropertyPath("two", RootWithSameLengthReferences.class), rootInsert, emptyMap(), IdValueSource.GENERATED);
		aggregateChange.addAction(twoInsert);

		MergedAggregateChange<RootWithSameLengthReferences, AggregateChangeWithRoot<RootWithSameLengthReferences>> change = //
				MutableAggregateChange.mergedSave(RootWithSameLengthReferences.class) //
						.merge(aggregateChange);

		List<DbAction<?>> actions = extractActions(change);
		assertThat(actions)
				.extracting(DbAction::getClass, DbAction::getEntityType, DbActionTestSupport::insertIdValueSource)
				.containsSubsequence( //
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED),
						Tuple.tuple(DbAction.BatchInsert.class, Intermediate.class, IdValueSource.GENERATED));
		List<DbAction.BatchInsert<Intermediate>> batchInsertActions = getBatchInsertActions(actions, Intermediate.class);
		assertThat(batchInsertActions).hasSize(2);
		assertThat(batchInsertActions.get(0).getActions()).containsExactly(oneInsert);
		assertThat(batchInsertActions.get(1).getActions()).containsExactly(twoInsert);
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

	private <T> List<DbAction<?>> extractActions(MergedAggregateChange<T, AggregateChangeWithRoot<T>> change) {

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