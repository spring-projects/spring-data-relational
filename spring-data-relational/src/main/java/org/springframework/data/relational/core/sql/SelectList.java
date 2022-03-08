/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.List;

import org.springframework.util.StringUtils;

/**
 * Value object representing the select list (selected columns, functions).
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class SelectList extends AbstractSegment {

	private final List<Expression> selectList;

	SelectList(List<Expression> selectList) {
		super(selectList.toArray(new Expression[0]));
		this.selectList = selectList;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return StringUtils.collectionToDelimitedString(selectList, ", ");
	}
}
