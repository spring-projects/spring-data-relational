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

/**
 * {@code TOP} clause for {@code SELECT TOP …}.
 *
 * @author Mark Paluch
 */
public class SelectTop extends AbstractSegment implements Segment {

	private final int count;

	private SelectTop(int count) {
		this.count = count;
	}

	public static SelectTop create(int count) {
		return new SelectTop(count);
	}

	/**
	 * @return the count.
	 */
	public int getCount() {
		return count;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TOP " + count;
	}
}
