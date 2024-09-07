/*
 * Copyright 2022-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.Subselect;

public class SubselectVisitor extends TypedSubtreeVisitor<Subselect> {

	private final RenderContext context;
	private final RenderTarget parent;

	private final SelectStatementVisitor delegate;
	private final StringBuilder builder = new StringBuilder("(");

	public SubselectVisitor(RenderContext context, RenderTarget parent) {

		this.context = context;
		this.parent = parent;

		this.delegate = new SelectStatementVisitor(context);
	}

	@Override
	Delegation enterMatched(Subselect segment) {
		return Delegation.delegateTo(delegate);
	}

	@Override
	Delegation leaveMatched(Subselect segment) {

		builder.append(delegate.getRenderedPart());
		builder.append(") ");

		parent.onRendered(builder);

		return super.leaveMatched(segment);
	}

}
