package org.springframework.data.relational.core.conversion;

import static java.util.Collections.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

public class SaveMergedAggregateChange<T> implements MergedAggregateChange<T, AggregateChangeWithRoot<T>> {

	private static final Comparator<PersistentPropertyPath<RelationalPersistentProperty>> pathLengthComparator = //
			Comparator.comparing(PersistentPropertyPath::getLength);

	private final Class<T> entityType;
	private final List<DbAction.WithRoot<?>> rootActions = new ArrayList<>();
	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, EnumMap<IdValueSource, List<DbAction.Insert<Object>>>> insertActions = //
			new HashMap<>();
	private final Map<PersistentPropertyPath<RelationalPersistentProperty>, List<DbAction.Delete<?>>> deleteActions = //
			new HashMap<>();

	public SaveMergedAggregateChange(Class<T> entityType) {
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

		Assert.notNull(consumer, "Consumer must not be null.");

		rootActions.forEach(consumer);
		deleteActions.entrySet().stream().sorted(Map.Entry.comparingByKey(pathLengthComparator.reversed()))
				.forEach((entry) -> entry.getValue().forEach(consumer));
		insertActions.entrySet().stream().sorted(Map.Entry.comparingByKey(pathLengthComparator))
				.forEach((entry) -> entry.getValue()
						.forEach((idValueSource, inserts) -> consumer.accept(new DbAction.InsertBatch<>(inserts, idValueSource))));
	}

	@Override
	public MergedAggregateChange<T, AggregateChangeWithRoot<T>> merge(AggregateChangeWithRoot<T> aggregateChange) {
		aggregateChange.forEachAction(action -> {
			if (action instanceof DbAction.WithRoot<?> rootAction) {
				rootActions.add(rootAction);
			} else if (action instanceof DbAction.Insert<?>) {
				// noinspection unchecked
				addInsert((DbAction.Insert<Object>) action);
			} else if (action instanceof DbAction.Delete<?> deleteAction) {
				addDelete(deleteAction);
			}
		});
		return this;
	}

	private void addInsert(DbAction.Insert<Object> action) {
		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = action.getPropertyPath();
		insertActions.merge(propertyPath,
				new EnumMap<>(singletonMap(action.getIdValueSource(), new ArrayList<>(singletonList(action)))),
				(enumMap, enumMapDefaultValue) -> {
					enumMap.merge(action.getIdValueSource(), new ArrayList<>(singletonList(action)),
							(actions, listDefaultValue) -> {
								actions.add(action);
								return actions;
							});
					return enumMap;
				});
	}

	private void addDelete(DbAction.Delete<?> action) {
		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = action.getPropertyPath();
		deleteActions.merge(propertyPath, new ArrayList<>(singletonList(action)), (actions, defaultValue) -> {
			actions.add(action);
			return actions;
		});
	}
}
