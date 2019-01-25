/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

/**
 * Abstract implementation to support {@link Segment} implementations.
 *
 * @author Mark Paluch
 */
abstract class AbstractSegment implements Segment {

	private final Segment[] children;

	protected AbstractSegment(Segment ... children) {
		this.children = children;
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null!");

		visitor.enter(this);
		for (Segment child : children) {
			child.visit(visitor);
		}
		visitor.leave(this);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return obj instanceof Segment && toString().equals(obj.toString());
	}
}
