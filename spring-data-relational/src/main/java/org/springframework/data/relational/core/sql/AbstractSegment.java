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
package org.springframework.data.relational.core.sql;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Abstract implementation to support {@link Segment} implementations.
 *
 * @author Mark Paluch
 * @since 1.1
 */
abstract class AbstractSegment implements Segment {

	private final Segment[] children;

	protected AbstractSegment(Segment... children) {
		this.children = children;
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null");

		visitor.enter(this);
		for (Segment child : children) {
			child.visit(visitor);
		}
		visitor.leave(this);
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(@Nullable Object obj) {
		return obj instanceof Segment && toString().equals(obj.toString());
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + StringUtils.arrayToDelimitedString(children, ", ") + ")";
	}
}
