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
package org.springframework.data.jdbc.core.convert;

import org.springframework.util.Assert;

/**
 * A collection of utility methods for dealing with arrays.
 * <p>
 * Mainly for internal use within the framework.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
final class ArrayUtilsToPrimitive {

	/**
	 * An empty immutable {@code boolean} array.
	 */
	public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];

	/**
	 * An empty immutable {@code byte} array.
	 */
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	/**
	 * An empty immutable {@code char} array.
	 */
	public static final char[] EMPTY_CHAR_ARRAY = new char[0];

	/**
	 * An empty immutable {@code double} array.
	 */
	public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

	/**
	 * An empty immutable {@code float} array.
	 */
	public static final float[] EMPTY_FLOAT_ARRAY = new float[0];

	/**
	 * An empty immutable {@code int} array.
	 */
	public static final int[] EMPTY_INT_ARRAY = new int[0];

	/**
	 * An empty immutable {@code long} array.
	 */
	public static final long[] EMPTY_LONG_ARRAY = new long[0];

	/**
	 * An empty immutable {@code short} array.
	 */
	public static final short[] EMPTY_SHORT_ARRAY = new short[0];

	private ArrayUtilsToPrimitive() {
	}

	/**
	 * Converts an {@code Boolean[]} into a {@code boolean[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code boolean[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static boolean[] toPrimitive(Boolean[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_BOOLEAN_ARRAY;
		}

		boolean[] booleans = new boolean[array.length];
		for (int i = 0; i < array.length; i++) {
			booleans[i] = array[i];
		}

		return booleans;
	}

	/**
	 * Converts an {@code Byte[]} into a {@code byte[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code byte[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static byte[] toPrimitive(Byte[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_BYTE_ARRAY;
		}

		byte[] bytes = new byte[array.length];
		for (int i = 0; i < array.length; i++) {
			bytes[i] = array[i];
		}

		return bytes;
	}

	/**
	 * Converts an {@code Character[]} into a {@code char[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code char[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static char[] toPrimitive(Character[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_CHAR_ARRAY;
		}

		char[] chars = new char[array.length];
		for (int i = 0; i < array.length; i++) {
			chars[i] = array[i];
		}

		return chars;
	}

	/**
	 * Converts an {@code Double[]} into a {@code double[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code double[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static double[] toPrimitive(Double[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_DOUBLE_ARRAY;
		}

		double[] doubles = new double[array.length];
		for (int i = 0; i < array.length; i++) {
			doubles[i] = array[i];
		}

		return doubles;
	}

	/**
	 * Converts an {@code Float[]} into a {@code float[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code float[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static float[] toPrimitive(Float[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_FLOAT_ARRAY;
		}

		float[] floats = new float[array.length];
		for (int i = 0; i < array.length; i++) {
			floats[i] = array[i];
		}

		return floats;
	}

	/**
	 * Converts an {@code Integer[]} into a {@code int[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code int[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static int[] toPrimitive(Integer[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_INT_ARRAY;
		}

		int[] ints = new int[array.length];
		for (int i = 0; i < array.length; i++) {
			ints[i] = array[i];
		}

		return ints;
	}

	/**
	 * Converts an {@code Long[]} into a {@code long[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code long[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static long[] toPrimitive(Long[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_LONG_ARRAY;
		}

		long[] longs = new long[array.length];
		for (int i = 0; i < array.length; i++) {
			longs[i] = array[i];
		}

		return longs;
	}

	/**
	 * Converts an {@code Short[]} into a {@code short[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null} and must not contain {@literal null} elements.
	 * @return a {@code short[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static short[] toPrimitive(Short[] array) {

		Assert.noNullElements(array, "Array must not contain null elements");

		if (array.length == 0) {
			return EMPTY_SHORT_ARRAY;
		}

		short[] shorts = new short[array.length];
		for (int i = 0; i < array.length; i++) {
			shorts[i] = array[i];
		}

		return shorts;
	}

}
