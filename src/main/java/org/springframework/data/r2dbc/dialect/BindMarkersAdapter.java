/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * Adapter to use Spring R2DBC's {@link org.springframework.r2dbc.core.binding.BindMarkers} exposing it as
 * {@link org.springframework.data.r2dbc.dialect.BindMarkers}.
 *
 * @author Mark Paluch
 * @since 1.2
 */
class BindMarkersAdapter implements org.springframework.data.r2dbc.dialect.BindMarkers {

	private final BindMarkers delegate;

	BindMarkersAdapter(BindMarkers delegate) {
		this.delegate = delegate;
	}

	@Override
	public org.springframework.data.r2dbc.dialect.BindMarker next() {
		return new BindMarkerAdapter(delegate.next());
	}

	@Override
	public org.springframework.data.r2dbc.dialect.BindMarker next(String hint) {
		return new BindMarkerAdapter(delegate.next());
	}

	static class BindMarkerAdapter implements org.springframework.data.r2dbc.dialect.BindMarker {

		private final BindMarker delegate;

		BindMarkerAdapter(BindMarker delegate) {
			this.delegate = delegate;
		}

		@Override
		public String getPlaceholder() {
			return delegate.getPlaceholder();
		}

		@Override
		public void bind(org.springframework.data.r2dbc.dialect.BindTarget bindTarget, Object value) {
			delegate.bind(bindTarget, value);
		}

		@Override
		public void bindNull(org.springframework.data.r2dbc.dialect.BindTarget bindTarget, Class<?> valueType) {
			delegate.bindNull(bindTarget, valueType);
		}

		@Override
		public void bind(BindTarget bindTarget, Object value) {
			delegate.bind(bindTarget, value);
		}

		@Override
		public void bindNull(BindTarget bindTarget, Class<?> valueType) {
			delegate.bindNull(bindTarget, valueType);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}
}
