/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

/**
 * Holder for an embedded {@link RelationalPersistentProperty}
 *
 * @author Mark Paluch
 * @since 3.2
 */
record EmbeddedContext(RelationalPersistentProperty ownerProperty) {

	EmbeddedContext {
	}

	public String getEmbeddedPrefix() {
		return ownerProperty.getEmbeddedPrefix();
	}

	public String withEmbeddedPrefix(String name) {

		if (!ownerProperty.isEmbedded()) {
			return name;
		}
		String embeddedPrefix = ownerProperty.getEmbeddedPrefix();
		if (embeddedPrefix != null) {
			return embeddedPrefix + name;
		}

		return name;
	}
}
