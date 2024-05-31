/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Matches an expression with an alias.
 *
 * @param pattern for the expression to match
 * @param alias to match
 * @author Jens Schauder
 */
record AliasedPattern(SelectItemPattern pattern, String alias) implements SelectItemPattern {

	@Override
	public boolean matches(SelectItem selectItem) {
		return pattern.matches(selectItem) && selectItem.getAlias() != null
				&& selectItem.getAlias().getName().equals(alias);
	}

	@Override
	public String toString() {
		return pattern + " as " + alias;
	}
}
