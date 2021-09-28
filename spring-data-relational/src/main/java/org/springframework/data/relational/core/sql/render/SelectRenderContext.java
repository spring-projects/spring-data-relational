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
package org.springframework.data.relational.core.sql.render;

import java.util.function.Function;

import org.springframework.data.relational.core.sql.Select;

/**
 * Render context specifically for {@code SELECT} statements. This interface declares rendering hooks that are called
 * before/after a specific {@code SELECT} clause part. The rendering content is appended directly after/before an
 * element without further whitespace processing. Hooks are responsible for adding required surrounding whitespaces.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @since 1.1
 */
public interface SelectRenderContext {

	/**
	 * Customization hook: Rendition of a part after the {@code SELECT} list and before any {@code FROM} renderings.
	 * Renders an empty string by default.
	 *
	 * @return render {@link Function} invoked after rendering {@code SELECT} list.
	 */
	default Function<Select, ? extends CharSequence> afterSelectList() {
		return select -> "";
	}

	/**
	 * Customization hook: Rendition of a part after {@code FROM} table. Renders an empty string by default.
	 *
	 * @return render {@link Function} invoked after rendering {@code FROM} table.
	 */
	default Function<Select, ? extends CharSequence> afterFromTable() {
		return select -> "";
	}

	/**
	 * Customization hook: Rendition of a part after {@code ORDER BY}. The rendering function is called always, regardless
	 * whether {@code ORDER BY} exists or not. Renders an empty string by default.
	 *
	 * @param hasOrderBy the actual value whether the {@link Select} statement has a {@code ORDER BY} clause.
	 * @return render {@link Function} invoked after rendering {@code ORDER BY}.
	 */
	default Function<Select, ? extends CharSequence> afterOrderBy(boolean hasOrderBy) {
		return select -> "";
	}
}
