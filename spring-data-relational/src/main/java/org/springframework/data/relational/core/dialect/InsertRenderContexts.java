package org.springframework.data.relational.core.dialect;

/**
 * In the scope of Insert with default values SQL statement, for example {@literal INSERT INTO SCHEMA.TABLE VALUES
 * (DEFAULT)} this enum represents the default values part in different {@link Dialect}s
 *
 * @author Mikhail Polivakha
 * @since 2.4
 */
public enum InsertRenderContexts implements InsertRenderContext {

	DEFAULT(" VALUES (DEFAULT)"), //
	MS_SQL_SERVER(" DEFAULT VALUES");

	private final String defaultInsertPart;

	InsertRenderContexts(String defaultInsertPart) {
		this.defaultInsertPart = defaultInsertPart;
	}

	public String getDefaultValuesInsertPart() {
		return defaultInsertPart;
	}

}
