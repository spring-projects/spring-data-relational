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

import java.util.Stack;

import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Visitor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Abstract base class for delegating {@link Visitor} implementations. This class implements a delegation pattern using
 * visitors. A delegating {@link Visitor} can implement {@link #doEnter(Visitable)} and {@link #doLeave(Visitable)}
 * methods to provide its functionality.
 * <p>
 * <h3>Delegation</h3> Typically, a {@link Visitor} is scoped to a single responsibility. If a {@link Visitor segment}
 * requires {@link #doEnter(Visitable) processing} that is not directly implemented by the visitor itself, the current
 * {@link Visitor} can delegate processing to a {@link DelegatingVisitor delegate}. Once a delegation is installed, the
 * {@link DelegatingVisitor delegate} is used as {@link Visitor} for the current and all subsequent items until it
 * {@link #doLeave(Visitable) signals} that it is no longer responsible.
 * </p>
 * <p>
 * Nested visitors are required to properly signal once they are no longer responsible for a {@link Visitor segment} to
 * step back from the delegation. Otherwise, parents are no longer involved in the visitation.
 * </p>
 * <p>
 * Delegation is recursive and limited by the stack size.
 * </p>
 * 
 * @author Mark Paluch
 * @since 1.1
 * @see FilteredSubtreeVisitor
 * @see TypedSubtreeVisitor
 */
abstract class DelegatingVisitor implements Visitor {

	private Stack<DelegatingVisitor> delegation = new Stack<>();

	/**
	 * Invoked for a {@link Visitable segment} when entering the segment.
	 * <p>
	 * This method can signal whether it is responsible for handling the {@link Visitor segment} or whether the segment
	 * requires delegation to a sub-{@link Visitor}. When delegating to a sub-{@link Visitor}, {@link #doEnter(Visitable)}
	 * is called on the {@link DelegatingVisitor delegate}.
	 * </p>
	 * 
	 * @param segment must not be {@literal null}.
	 * @return
	 */
	@Nullable
	public abstract Delegation doEnter(Visitable segment);

	@Override
	public final void enter(Visitable segment) {

		if (delegation.isEmpty()) {

			Delegation visitor = doEnter(segment);
			Assert.notNull(visitor,
					() -> String.format("Visitor must not be null Caused by %s.doEnter(…)", getClass().getName()));
			Assert.state(!visitor.isLeave(),
					() -> String.format("Delegation indicates leave. Caused by %s.doEnter(…)", getClass().getName()));

			if (visitor.isDelegate()) {
				delegation.push(visitor.getDelegate());
				visitor.getDelegate().enter(segment);
			}
		} else {
			delegation.peek().enter(segment);
		}
	}

	/**
	 * Invoked for a {@link Visitable segment} when leaving the segment.
	 * <p>
	 * This method can signal whether this {@link Visitor} should remain responsible for handling subsequent
	 * {@link Visitor segments} or whether it should step back from delegation. When stepping back from delegation,
	 * {@link #doLeave(Visitable)} is called on the {@link DelegatingVisitor parent delegate}.
	 * </p>
	 * 
	 * @param segment must not be {@literal null}.
	 * @return
	 */
	public abstract Delegation doLeave(Visitable segment);

	public final void leave(Visitable segment) {
		doLeave0(segment);
	}

	private Delegation doLeave0(Visitable segment) {

		if (delegation.isEmpty()) {
			return doLeave(segment);
		} else {

			DelegatingVisitor visitor = delegation.peek();
			while (visitor != null) {

				Delegation result = visitor.doLeave0(segment);
				Assert.notNull(visitor,
						() -> String.format("Visitor must not be null Caused by %s.doLeave(…)", getClass().getName()));

				if (visitor == this) {
					if (result.isLeave()) {
						return delegation.isEmpty() ? Delegation.leave() : Delegation.retain();
					}
					return Delegation.retain();
				}

				if (result.isRetain()) {
					return result;
				}

				if (result.isLeave()) {

					if (!delegation.isEmpty()) {
						delegation.pop();
					}

					if (!delegation.isEmpty()) {
						visitor = delegation.peek();
					} else {
						visitor = this;
					}
				}
			}
		}

		return Delegation.leave();
	}

	/**
	 * Value object to control delegation.
	 */
	static class Delegation {

		private static Delegation RETAIN = new Delegation(true, false, null);
		private static Delegation LEAVE = new Delegation(false, true, null);

		private final boolean retain;
		private final boolean leave;

		private final @Nullable DelegatingVisitor delegate;

		private Delegation(boolean retain, boolean leave, @Nullable DelegatingVisitor delegate) {
			this.retain = retain;
			this.leave = leave;
			this.delegate = delegate;
		}

		public static Delegation retain() {
			return RETAIN;
		}

		public static Delegation leave() {
			return LEAVE;
		}

		public static Delegation delegateTo(DelegatingVisitor visitor) {
			return new Delegation(false, false, visitor);
		}

		boolean isDelegate() {
			return delegate != null;
		}

		boolean isRetain() {
			return retain;
		}

		boolean isLeave() {
			return leave;
		}

		DelegatingVisitor getDelegate() {

			Assert.state(isDelegate(), "No delegate available");
			return delegate;
		}
	}
}
