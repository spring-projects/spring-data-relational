package org.springframework.data.r2dbc.dialect;

/**
 * An SQL dialect for Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
public class SqlServerDialect implements Dialect {

	/**
	 * Singleton instance.
	 */
	public static final SqlServerDialect INSTANCE = new SqlServerDialect();

	private static final BindMarkersFactory NAMED = BindMarkersFactory.named("@", "P", 32,
			SqlServerDialect::filterBindMarker);

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long)
		 */
		@Override
		public String getClause(long limit) {
			return "OFFSET 0 ROWS FETCH NEXT " + limit + " ROWS ONLY";
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getClause(long limit, long offset) {
			return String.format("OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.END;
		}
	};

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getBindMarkersFactory()
	 */
	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return NAMED;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#returnGeneratedKeys()
	 */
	@Override
	public String returnGeneratedKeys() {
		return "select SCOPE_IDENTITY() AS GENERATED_KEYS";
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#limit()
	 */
	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	private static String filterBindMarker(CharSequence input) {

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < input.length(); i++) {

			char ch = input.charAt(i);

			// ascii letter or digit
			if (Character.isLetterOrDigit(ch) && ch < 127) {
				builder.append(ch);
			}
		}

		if (builder.length() == 0) {
			return "";
		}

		return "_" + builder.toString();
	}
}
