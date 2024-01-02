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
package org.springframework.data.relational.core.sql;

/**
 * Baseclass for all kinds of "select in parenthesis".
 * 
 * @since 3.1
 * @author Mark Paluch
 * @author Jens Schauder
 */
public abstract class Subselect extends AbstractSegment {

	private final Select select;

	protected Subselect(Select select) {

		super(select);

		this.select = select;
	}

	public Select getSelect() {
		return select;
	}

	@Override
	public String toString() {
		return "(" + select + ")";
	}

}
