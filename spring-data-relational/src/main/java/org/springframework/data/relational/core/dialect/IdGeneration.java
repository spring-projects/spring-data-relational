/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Encapsulates various properties that are related to ID generation process and specific to given {@link Dialect}
 *
 * @author Jens Schauder
 * @author Chirag Tailor
 * @author Mikhail Polivakha
 * @since 2.1
 */
public interface IdGeneration {

	static IdGeneration create(IdentifierProcessing identifierProcessing) {

		return new IdGeneration() {

			@Override
			public String createSequenceQuery(SqlIdentifier sequenceName) {
				return IdGeneration.createSequenceQuery(sequenceName.toSql(identifierProcessing));
			}
		};
	}

	/**
	 * A default instance working for many databases and equivalent to Spring Data JDBCs behavior before version 2.1.
	 */
	IdGeneration DEFAULT = new IdGeneration() {};

	/**
	 * Does the driver require the specification of those columns for which a generated id shall be returned.
	 * <p>
	 * This should be {@literal false} for most dialects. One notable exception is Oracle.
	 *
	 * @return {@literal true} if the a list of column names should get passed to the JDBC driver for which ids shall be
	 *         generated.
	 * @see Connection#prepareStatement(String, String[])
	 */
	default boolean driverRequiresKeyColumnNames() {
		return false;
	}

	/**
	 * Provides for a given id {@link SqlIdentifier} the String that is to be used for registering interest in the
	 * generated value of that column.
	 *
	 * @param id {@link SqlIdentifier} representing a column for which a generated value is to be obtained.
	 * @return a String representing that column in the way expected by the JDBC driver.
	 * @since 3.3
	 */
	default String getKeyColumnName(SqlIdentifier id) {
		return id.getReference();
	}

	/**
	 * Does the driver support id generation for batch operations.
	 * <p>
	 * This should be {@literal true} for most dialects, except DB2 and SqlServer.
	 *
	 * @return {@literal true} if the JDBC driver supports generated keys for batch operations.
	 * @see PreparedStatement#getGeneratedKeys()
	 * @since 2.4
	 */
	default boolean supportedForBatchOperations() {
		return true;
	}

	/**
	 * @return {@literal true} in case the sequences are supported by the underlying database, {@literal false} otherwise
	 * @since 3.5
	 */
	default boolean sequencesSupported() {
		return true;
	}

	/**
	 * The SQL statement that allows retrieving the next value from the passed sequence
	 *
	 * @param sequenceName the sequence name to get the next value for
	 * @return SQL string
	 * @since 3.5
	 */
	default String createSequenceQuery(SqlIdentifier sequenceName) {

		String nameString = sequenceName.toString();
		return createSequenceQuery(nameString);
	}

	static String createSequenceQuery(String nameString) {
		return "SELECT NEXT VALUE FOR " + nameString;
	}
}
