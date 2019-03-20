/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.Statement;

/**
 * A single indexed bind marker.
 */
class IndexedBindMarker implements BindMarker {

	private final String placeholder;

	private int index;

	IndexedBindMarker(String placeholder, int index) {
		this.placeholder = placeholder;
		this.index = index;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarker#getPlaceholder()
	 */
	@Override
	public String getPlaceholder() {
		return placeholder;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindValue(io.r2dbc.spi.Statement, java.lang.Object)
	 */
	@Override
	public void bind(Statement statement, Object value) {
		statement.bind(this.index, value);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.BindMarker#bindNull(io.r2dbc.spi.Statement, java.lang.Class)
	 */
	@Override
	public void bindNull(Statement statement, Class<?> valueType) {
		statement.bindNull(this.index, valueType);
	}
}
