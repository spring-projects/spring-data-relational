package org.springframework.data.relational.core.conversion;

public interface MergedAggregateChange<T, C extends MutableAggregateChange<T>> extends AggregateChange<T> {
	MergedAggregateChange<T, C> merge(C aggregateChange);
}
