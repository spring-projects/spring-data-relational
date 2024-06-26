/*
 * Copyright 2024 the original author or authors.
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

import static org.mockito.Mockito.*;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

class EmbeddedRelationalPersistentPropertyUnitTests {

	@Test // GH-1694
	void testEquals() {

		RelationalPersistentProperty delegate = mock(RelationalPersistentProperty.class);
		EmbeddedRelationalPersistentProperty embeddedProperty = new EmbeddedRelationalPersistentProperty(delegate, mock(EmbeddedContext.class));

		RelationalPersistentProperty otherDelegate = mock(RelationalPersistentProperty.class);
		EmbeddedRelationalPersistentProperty otherEmbeddedProperty = new EmbeddedRelationalPersistentProperty(otherDelegate, mock(EmbeddedContext.class));

		SoftAssertions.assertSoftly(softly -> {
			softly.assertThat(embeddedProperty).isEqualTo(embeddedProperty);
			softly.assertThat(embeddedProperty).isEqualTo(delegate);

			softly.assertThat(embeddedProperty).isNotEqualTo(otherEmbeddedProperty);
			softly.assertThat(embeddedProperty).isNotEqualTo(otherDelegate);
		});
	}

}