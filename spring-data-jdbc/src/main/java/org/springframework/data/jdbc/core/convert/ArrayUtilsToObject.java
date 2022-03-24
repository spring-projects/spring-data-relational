package org.springframework.data.jdbc.core.convert;

import org.springframework.util.Assert;

public class ArrayUtilsToObject {
    /**
     * An empty immutable {@link Boolean} array.
     */
    public static final Boolean[] EMPTY_BOOLEAN_OBJECT_ARRAY = new Boolean[0];

    /**
     * An empty immutable {@link Byte} array.
     */
    public static final Byte[] EMPTY_BYTE_OBJECT_ARRAY = new Byte[0];

    /**
     * An empty immutable {@link Character} array.
     */
    public static final Character[] EMPTY_CHARACTER_OBJECT_ARRAY = new Character[0];

    /**
     * An empty immutable {@code Double} array.
     */
    public static final Double[] EMPTY_DOUBLE_OBJECT_ARRAY = new Double[0];

    /**
     * An empty immutable {@code Float} array.
     */
    public static final Float[] EMPTY_FLOAT_OBJECT_ARRAY = new Float[0];

    /**
     * An empty immutable {@link Integer} array.
     */
    public static final Integer[] EMPTY_INTEGER_OBJECT_ARRAY = new Integer[0];

    /**
     * An empty immutable {@link Long} array.
     */
    public static final Long[] EMPTY_LONG_OBJECT_ARRAY = new Long[0];

    /**
     * An empty immutable {@link Short} array.
     */
    public static final Short[] EMPTY_SHORT_OBJECT_ARRAY = new Short[0];

    private ArrayUtilsToObject() {
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
