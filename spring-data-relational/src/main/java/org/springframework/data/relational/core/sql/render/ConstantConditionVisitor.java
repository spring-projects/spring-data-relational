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

import org.springframework.data.relational.core.sql.ConstantCondition;

/**
 * Renderer for {@link ConstantCondition}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Daniele Canteri
 * @since 2.3
 */
class ConstantConditionVisitor extends TypedSingleConditionRenderSupport<ConstantCondition> {

	private final RenderTarget target;

	ConstantConditionVisitor(RenderContext context, RenderTarget target) {
		super(context);
		this.target = target;
	}

	@Override
	Delegation leaveMatched(ConstantCondition segment) {

		target.onRendered(segment.toString());

		return super.leaveMatched(segment);
	}

}
