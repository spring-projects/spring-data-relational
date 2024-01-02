/*
 * Copyright 2019-2024 the original author or authors.
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
final class ArrayUtils {

	/**
	 * An empty immutable {@code boolean} array.
	 */
	public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];

	/**
	 * An empty immutable {@link Boolean} array.
	 */
	public static final Boolean[] EMPTY_BOOLEAN_OBJECT_ARRAY = new Boolean[0];

	/**
	 * An empty immutable {@code byte} array.
	 */
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	/**
	 * An empty immutable {@link Byte} array.
	 */
	public static final Byte[] EMPTY_BYTE_OBJECT_ARRAY = new Byte[0];

	/**
	 * An empty immutable {@code char} array.
	 */
	public static final char[] EMPTY_CHAR_ARRAY = new char[0];

	/**
	 * An empty immutable {@link Character} array.
	 */
	public static final Character[] EMPTY_CHARACTER_OBJECT_ARRAY = new Character[0];

	/**
	 * An empty immutable {@code double} array.
	 */
	public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];

	/**
	 * An empty immutable {@code Double} array.
	 */
	public static final Double[] EMPTY_DOUBLE_OBJECT_ARRAY = new Double[0];

	/**
	 * An empty immutable {@code float} array.
	 */
	public static final float[] EMPTY_FLOAT_ARRAY = new float[0];

	/**
	 * An empty immutable {@code Float} array.
	 */
	public static final Float[] EMPTY_FLOAT_OBJECT_ARRAY = new Float[0];

	/**
	 * An empty immutable {@code int} array.
	 */
	public static final int[] EMPTY_INT_ARRAY = new int[0];

	/**
	 * An empty immutable {@link Integer} array.
	 */
	public static final Integer[] EMPTY_INTEGER_OBJECT_ARRAY = new Integer[0];

	/**
	 * An empty immutable {@code long} array.
	 */
	public static final long[] EMPTY_LONG_ARRAY = new long[0];

	/**
	 * An empty immutable {@link Long} array.
	 */
	public static final Long[] EMPTY_LONG_OBJECT_ARRAY = new Long[0];

	/**
	 * An empty immutable {@code short} array.
	 */
	public static final short[] EMPTY_SHORT_ARRAY = new short[0];

	/**
	 * An empty immutable {@link Short} array.
	 */
	public static final Short[] EMPTY_SHORT_OBJECT_ARRAY = new Short[0];

	private ArrayUtils() {
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
	 * Converts an {@code boolean[]} into a {@code Boolean[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Boolean[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Boolean[] toObject(boolean[] array) {

		if (array.length == 0) {
			return EMPTY_BOOLEAN_OBJECT_ARRAY;
		}

		Boolean[] booleans = new Boolean[array.length];
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
	 * Converts an {@code byte[]} into a {@code Byte[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Byte[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Byte[] toObject(byte[] array) {

		if (array.length == 0) {
			return EMPTY_BYTE_OBJECT_ARRAY;
		}

		Byte[] bytes = new Byte[array.length];
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
	 * Converts an {@code char[]} into a {@code Character[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Character[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Character[] toObject(char[] array) {

		if (array.length == 0) {
			return EMPTY_CHARACTER_OBJECT_ARRAY;
		}

		Character[] objects = new Character[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}
		return objects;
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
	 * Converts an {@code double[]} into a {@code Double[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Double[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Double[] toObject(double[] array) {

		if (array.length == 0) {
			return EMPTY_DOUBLE_OBJECT_ARRAY;
		}

		Double[] objects = new Double[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}

		return objects;
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
	 * Converts an {@code float[]} into a {@code Float[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Float[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Float[] toObject(float[] array) {

		if (array.length == 0) {
			return EMPTY_FLOAT_OBJECT_ARRAY;
		}

		Float[] objects = new Float[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}

		return objects;
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
	 * Converts an {@code int[]} into a {@code Integer[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Integer[]} of same size with the boxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Integer[] toObject(int[] array) {

		if (array.length == 0) {
			return EMPTY_INTEGER_OBJECT_ARRAY;
		}

		Integer[] objects = new Integer[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}

		return objects;
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
	 * Converts an {@code long[]} into a {@code Long[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Long[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Long[] toObject(long[] array) {

		if (array.length == 0) {
			return EMPTY_LONG_OBJECT_ARRAY;
		}

		Long[] objects = new Long[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}
		return objects;
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

	/**
	 * Converts an {@code short[]} into a {@code Short[]}.
	 *
	 * @param array the array to be converted. Must not be {@literal null}.
	 * @return a {@code Short[]} of same size with the unboxed values of the input array. Guaranteed to be not
	 *         {@literal null}.
	 */
	static Short[] toObject(short[] array) {

		if (array.length == 0) {
			return EMPTY_SHORT_OBJECT_ARRAY;
		}

		Short[] objects = new Short[array.length];
		for (int i = 0; i < array.length; i++) {
			objects[i] = array[i];
		}

		return objects;
	}

}
