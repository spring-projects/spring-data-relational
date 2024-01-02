/*
 * Copyright 2021-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.SegmentList;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.util.Assert;

/**
 * A part rendering visitor for lists of segments. It can be set up depending on the elements in the list it should
 * handle and the way elemnts should get separated when rendered.
 * 
 * @author Jens Schauder
 * @since 2.7
 */
class SegmentListVisitor extends TypedSubtreeVisitor<SegmentList<?>> implements PartRenderer {

	private final StringBuilder part = new StringBuilder();
	private final String start;
	private final String separator;
	private final DelegatingVisitor nestedVisitor;

	private boolean first = true;

	/**
	 * @param start a {@literal String} to be rendered before the first element if there is at least one element. Must not
	 *          be {@literal null}.
	 * @param separator a {@literal String} to be rendered between elements. Must not be {@literal null}.
	 * @param nestedVisitor the {@link org.springframework.data.relational.core.sql.Visitor} responsible for rendering the
	 *          elements of the list. Must not be {@literal null}.
	 */
	SegmentListVisitor(String start, String separator, DelegatingVisitor nestedVisitor) {

		Assert.notNull(start, "Start must not be null");
		Assert.notNull(separator, "Separator must not be null");
		Assert.notNull(nestedVisitor, "Nested Visitor must not be null");
		Assert.isInstanceOf(PartRenderer.class, nestedVisitor, "Nested visitor must implement PartRenderer");

		this.start = start;
		this.separator = separator;
		this.nestedVisitor = nestedVisitor;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (first) {
			part.append(start);
			first = false;
		} else {
			part.append(separator);
		}

		return Delegation.delegateTo(nestedVisitor);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		part.append(((PartRenderer) nestedVisitor).getRenderedPart());

		return super.leaveNested(segment);
	}

	@Override
	public CharSequence getRenderedPart() {

		return part;
	}
}
