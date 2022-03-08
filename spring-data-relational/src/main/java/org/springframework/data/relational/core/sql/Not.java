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

/**
 * @author Jens Schauder
 * @since 1.1
 */
public class Not extends AbstractSegment implements Condition {

	private final Condition condition;

	Not(Condition condition) {

		super(condition);

		this.condition = condition;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Condition#not()
	 */
	@Override
	public Condition not() {
		return condition;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "NOT " + condition;
	}
}
