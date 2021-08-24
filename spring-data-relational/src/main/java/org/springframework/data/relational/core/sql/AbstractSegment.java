/*
 * Copyright 2019-2021 the original author or authors.
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

import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implementation to support {@link Segment} implementations.
 *
 * @author Mark Paluch
 * @since 1.1
 */
abstract class AbstractSegment implements Segment {

	private final Segment[] children;

	protected AbstractSegment(Segment... children) {
		this.children = toSegmentArray(children);
	}

	private Segment[] toSegmentArray(Segment... children) {
		List<Segment> list = new ArrayList<>();
		if (children != null) {
			for (Segment child : children) {
				if (child != null) {
					list.add(child);
				}
			}
		}
		return list.toArray(new Segment[list.size()]);
	}


	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
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
