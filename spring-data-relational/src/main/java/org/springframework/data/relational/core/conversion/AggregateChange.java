package org.springframework.data.relational.core.conversion;

import java.util.function.Consumer;

import org.springframework.lang.Nullable;

public interface AggregateChange<T> {

	/**
	 * Applies the given consumer to each {@link DbAction} in this {@code AggregateChange}.
	 *
	 * @param consumer must not be {@literal null}.
	 */
	void forEachAction(Consumer<? super DbAction<?>> consumer);

	/**
	 * Returns the {@link Kind} of {@code AggregateChange} this is.
	 *
	 * @return guaranteed to be not {@literal null}.
	 */
	Kind getKind();

	/**
	 * The type of the root of this {@code AggregateChange}.
	 *
	 * @return Guaranteed to be not {@literal null}.
	 */
	Class<T> getEntityType();

	/**
	 * The entity to which this {@link AggregateChange} relates.
	 *
	 * @return may be {@literal null}.
	 */
	@Nullable
	T getEntity();

	/**
	 * The kind of action to be performed on an aggregate.
	 */
	enum Kind {
		/**
		 * A {@code SAVE} of an aggregate typically involves an {@code insert} or {@code update} on the aggregate root plus
		 * {@code insert}s, {@code update}s, and {@code delete}s on the other elements of an aggregate.
		 */
		SAVE,

		/**
		 * A {@code DELETE} of an aggregate typically involves a {@code delete} on all contained entities.
		 */
		DELETE
	}
}
