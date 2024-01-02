/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import java.util.function.Predicate;

import org.springframework.core.ResolvableType;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Visitor;
import org.springframework.lang.Nullable;
import org.springframework.util.ConcurrentReferenceHashMap;

/**
 * Type-filtering {@link DelegatingVisitor visitor} applying a {@link Class type filter} derived from the generic type
 * parameter. Typically used as base class for {@link Visitor visitors} that wish to apply hierarchical processing based
 * on a well-defined entry {@link Visitor segment}.
 * <p>
 * Filtering is a three-way process:
 * <ol>
 * <li>Ignores elements that do not match the filter {@link Predicate}.</li>
 * <li>{@link #enterMatched(Visitable) enter}/{@link #leaveMatched(Visitable) leave} matched callbacks for the
 * {@link Visitable segment} that matches the {@link Predicate}.</li>
 * <li>{@link #enterNested(Visitable) enter}/{@link #leaveNested(Visitable) leave} nested callbacks for direct/nested
 * children of the matched {@link Visitable} until {@link #leaveMatched(Visitable) leaving the matched}
 * {@link Visitable}.</li>
 * </ol>
 * </p>
 *
 * @author Mark Paluch
 * @since 1.1
 * @see FilteredSubtreeVisitor
 */
abstract class TypedSubtreeVisitor<T extends Visitable> extends DelegatingVisitor {

	private static final ConcurrentReferenceHashMap<Class<?>, ResolvableType> refCache = new ConcurrentReferenceHashMap<>();

	private final ResolvableType type;
	private @Nullable Visitable currentSegment;

	enum Assignable {
		YES, NO,
	}

	/**
	 * Creates a new {@link TypedSubtreeVisitor}.
	 */
	TypedSubtreeVisitor() {
		this.type = refCache.computeIfAbsent(this.getClass(),
				key -> ResolvableType.forClass(key).as(TypedSubtreeVisitor.class).getGeneric(0));
	}

	/**
	 * Creates a new {@link TypedSubtreeVisitor} with an explicitly provided type.
	 */
	TypedSubtreeVisitor(Class<T> type) {
		this.type = refCache.computeIfAbsent(type, key -> ResolvableType.forClass(type));
	}

	/**
	 * {@link Visitor#enter(Visitable) Enter} callback for a {@link Visitable} that this {@link Visitor} is responsible
	 * for. The default implementation retains delegation by default.
	 *
	 * @param segment the segment, must not be {@literal null}.
	 * @return delegation options. Can be either {@link Delegation#retain()} or
	 *         {@link Delegation#delegateTo(DelegatingVisitor)}.
	 * @see Delegation#retain()
	 */
	Delegation enterMatched(T segment) {
		return Delegation.retain();
	}

	/**
	 * {@link Visitor#enter(Visitable) Enter} callback for a nested {@link Visitable}. The default implementation retains
	 * delegation by default.
	 *
	 * @param segment the segment, must not be {@literal null}.
	 * @return delegation options. Can be either {@link Delegation#retain()} or
	 *         {@link Delegation#delegateTo(DelegatingVisitor)}.
	 * @see Delegation#retain()
	 */
	Delegation enterNested(Visitable segment) {
		return Delegation.retain();
	}

	/**
	 * {@link Visitor#leave(Visitable) Leave} callback for the matched {@link Visitable}. The default implementation steps
	 * back from delegation by default.
	 *
	 * @param segment the segment, must not be {@literal null}.
	 * @return delegation options. Can be either {@link Delegation#retain()} or {@link Delegation#leave()}.
	 * @see Delegation#leave()
	 */
	Delegation leaveMatched(T segment) {
		return Delegation.leave();
	}

	/**
	 * {@link Visitor#leave(Visitable) Leave} callback for a nested {@link Visitable}. The default implementation retains
	 * delegation by default.
	 *
	 * @param segment the segment, must not be {@literal null}.
	 * @return delegation options. Can be either {@link Delegation#retain()} or {@link Delegation#leave()}.
	 * @see Delegation#retain()
	 */
	Delegation leaveNested(Visitable segment) {
		return Delegation.retain();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Delegation doEnter(Visitable segment) {

		if (currentSegment == null) {

			if (type.isInstance(segment)) {

				currentSegment = segment;
				return enterMatched((T) segment);
			}
		} else {
			return enterNested(segment);
		}

		return Delegation.retain();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final Delegation doLeave(Visitable segment) {

		if (currentSegment == null) {
			return Delegation.leave();
		} else if (segment == currentSegment) {
			currentSegment = null;
			return leaveMatched((T) segment);
		} else {
			return leaveNested(segment);
		}
	}

}
