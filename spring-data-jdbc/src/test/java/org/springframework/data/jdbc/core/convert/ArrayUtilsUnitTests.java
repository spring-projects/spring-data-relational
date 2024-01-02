/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ArrayUtils}.
 *
 * @author Mark Paluch
 */
class ArrayUtilsUnitTests {

	@Test
	void testCreatePrimitiveArray() {

		assertThat(ArrayUtils.toPrimitive(new Boolean[] { true })).isEqualTo(new boolean[] { true });
		assertThat(ArrayUtils.toPrimitive(new Byte[] { 1 })).isEqualTo(new byte[] { 1 });
		assertThat(ArrayUtils.toPrimitive(new Character[] { 'a' })).isEqualTo(new char[] { 'a' });
		assertThat(ArrayUtils.toPrimitive(new Double[] { 2.718 })).contains(new double[] { 2.718 }, offset(0.1));
		assertThat(ArrayUtils.toPrimitive(new Float[] { 3.14f })).contains(new float[] { 3.14f }, offset(0.1f));
		assertThat(ArrayUtils.toPrimitive(new Integer[] {})).isEqualTo(new int[] {});
		assertThat(ArrayUtils.toPrimitive(new Long[] { 2L, 3L })).isEqualTo(new long[] { 2, 3 });
		assertThat(ArrayUtils.toPrimitive(new Short[] { 2 })).isEqualTo(new short[] { 2 });
	}

	@Test
	void testCreatePrimitiveArrayViaObjectArray() {

		assertThat(ArrayUtils.toPrimitive(new Boolean[] { true })).isEqualTo(new boolean[] { true });
		assertThat(ArrayUtils.toPrimitive(new Byte[] { 1 })).isEqualTo(new byte[] { 1 });
		assertThat(ArrayUtils.toPrimitive(new Character[] { 'a' })).isEqualTo(new char[] { 'a' });
		assertThat(ArrayUtils.toPrimitive(new Double[] { 2.718 })).contains(new double[] { 2.718 }, offset(0.1));
		assertThat(ArrayUtils.toPrimitive(new Float[] { 3.14f })).contains(new float[] { 3.14f }, offset(0.1f));
		assertThat(ArrayUtils.toPrimitive(new Integer[] {})).isEqualTo(new int[] {});
		assertThat(ArrayUtils.toPrimitive(new Long[] { 2L, 3L })).isEqualTo(new long[] { 2, 3 });
		assertThat(ArrayUtils.toPrimitive(new Short[] { 2 })).isEqualTo(new short[] { 2 });
	}
}
