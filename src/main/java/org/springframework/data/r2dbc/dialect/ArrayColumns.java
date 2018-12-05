package org.springframework.data.r2dbc.dialect;

/**
 * Interface declaring methods that express how a dialect supports array-typed columns.
 *
 * @author Mark Paluch
 */
public interface ArrayColumns {

	/**
	 * Returns {@literal true} if the dialect supports array-typed columns.
	 *
	 * @return {@literal true} if the dialect supports array-typed columns.
	 */
	boolean isSupported();

	/**
	 * Translate the {@link Class user type} of an array into the dialect-specific type. This method considers only the
	 * component type.
	 *
	 * @param userType component type of the array.
	 * @return the dialect-supported array type.
	 * @throws UnsupportedOperationException if array typed columns are not supported.
	 * @throws IllegalArgumentException if the {@code userType} is not a supported array type.
	 */
	Class<?> getArrayType(Class<?> userType);

	/**
	 * Default {@link ArrayColumns} implementation for dialects that do not support array-typed columns.
	 */
	enum Unsupported implements ArrayColumns {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.ArrayColumns#isSupported()
		 */
		@Override
		public boolean isSupported() {
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.ArrayColumns#getArrayType(java.lang.Class)
		 */
		@Override
		public Class<?> getArrayType(Class<?> userType) {
			throw new UnsupportedOperationException("Array types not supported");
		}
	}
}
