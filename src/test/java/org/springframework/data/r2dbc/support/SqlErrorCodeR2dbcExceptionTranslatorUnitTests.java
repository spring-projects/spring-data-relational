/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.r2dbc.support;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.R2dbcException;

import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.BadSqlGrammarException;
import org.springframework.data.r2dbc.InvalidResultAccessException;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;
import org.springframework.jdbc.support.SQLErrorCodes;

/**
 * Unit tests for {@link SqlErrorCodeR2dbcExceptionTranslator}.
 *
 * @author Mark Paluch
 */
class SqlErrorCodeR2dbcExceptionTranslatorUnitTests {

	private static final SQLErrorCodes ERROR_CODES = new SQLErrorCodes();
	static {
		ERROR_CODES.setBadSqlGrammarCodes("1", "2");
		ERROR_CODES.setInvalidResultSetAccessCodes("3", "4");
		ERROR_CODES.setDuplicateKeyCodes("10");
		ERROR_CODES.setDataAccessResourceFailureCodes("5");
		ERROR_CODES.setDataIntegrityViolationCodes("6");
		ERROR_CODES.setCannotAcquireLockCodes("7");
		ERROR_CODES.setDeadlockLoserCodes("8");
		ERROR_CODES.setCannotSerializeTransactionCodes("9");
	}

	@Test
	void shouldTranslateToBadGrammarException() {

		R2dbcExceptionTranslator sut = new SqlErrorCodeR2dbcExceptionTranslator(ERROR_CODES);

		R2dbcException cause = new MyR2dbcException("", "", 1);
		BadSqlGrammarException exception = (BadSqlGrammarException) sut.translate("task", "SQL", cause);

		assertThat(exception.getSql()).isEqualTo("SQL");
		assertThat(exception.getR2dbcException()).isEqualTo(cause);
	}

	@Test
	void shouldTranslateToResultException() {

		R2dbcExceptionTranslator sut = new SqlErrorCodeR2dbcExceptionTranslator(ERROR_CODES);

		R2dbcException cause = new MyR2dbcException("", "", 4);
		InvalidResultAccessException exception = (InvalidResultAccessException) sut.translate("task", "SQL", cause);

		assertThat(exception.getSql()).isEqualTo("SQL");
		assertThat(exception.getR2dbcException()).isEqualTo(cause);
	}

	@Test
	void shouldFallbackToUncategorized() {

		R2dbcExceptionTranslator sut = new SqlErrorCodeR2dbcExceptionTranslator(ERROR_CODES);

		// Test fallback. We assume that no database will ever return this error code,
		// but 07xxx will be bad grammar picked up by the fallback SQLState translator
		R2dbcException cause = new MyR2dbcException("", "07xxx", 666666666);
		UncategorizedR2dbcException exception = (UncategorizedR2dbcException) sut.translate("task", "SQL2", cause);

		assertThat(exception.getSql()).isEqualTo("SQL2");
		assertThat(exception.getR2dbcException()).isEqualTo(cause);
	}

	@Test
	void shouldTranslateDataIntegrityViolationException() {

		R2dbcExceptionTranslator sut = new SqlErrorCodeR2dbcExceptionTranslator(ERROR_CODES);

		R2dbcException cause = new MyR2dbcException("", "", 10);
		DataAccessException exception = sut.translate("task", "SQL", cause);

		assertThat(exception).isInstanceOf(DataIntegrityViolationException.class);
	}

	@Test
	void errorCodeTranslation() {

		R2dbcExceptionTranslator sut = new SqlErrorCodeR2dbcExceptionTranslator(ERROR_CODES);

		checkTranslation(sut, 5, DataAccessResourceFailureException.class);
		checkTranslation(sut, 6, DataIntegrityViolationException.class);
		checkTranslation(sut, 7, CannotAcquireLockException.class);
		checkTranslation(sut, 8, DeadlockLoserDataAccessException.class);
		checkTranslation(sut, 9, CannotSerializeTransactionException.class);
		checkTranslation(sut, 10, DuplicateKeyException.class);
	}

	private static void checkTranslation(R2dbcExceptionTranslator sext, int errorCode, Class<?> exClass) {

		R2dbcException cause = new MyR2dbcException("", "", errorCode);
		DataAccessException exception = sext.translate("", "", cause);

		assertThat(exception).isInstanceOf(exClass).hasCause(cause);
	}

	@Test
	void shouldApplyCustomTranslation() {

		String TASK = "TASK";
		String SQL = "SQL SELECT *";
		DataAccessException custom = new DataAccessException("") {};

		R2dbcException cause = new MyR2dbcException("", "", 1);
		R2dbcException intVioEx = new MyR2dbcException("", "", 6);

		SqlErrorCodeR2dbcExceptionTranslator translator = new SqlErrorCodeR2dbcExceptionTranslator() {
			@Override
			protected DataAccessException customTranslate(String task, String sql, R2dbcException sqlex) {

				assertThat(task).isEqualTo(TASK);
				assertThat(sql).isEqualTo(SQL);
				return (sqlex == cause) ? custom : null;
			}
		};
		translator.setSqlErrorCodes(ERROR_CODES);

		// Shouldn't custom translate this
		assertThat(translator.translate(TASK, SQL, cause)).isEqualTo(custom);

		DataIntegrityViolationException diex = (DataIntegrityViolationException) translator.translate(TASK, SQL, intVioEx);
		assertThat(diex).hasCause(intVioEx);
	}

	static class MyR2dbcException extends R2dbcException {

		MyR2dbcException(String reason, String sqlState, int errorCode) {
			super(reason, sqlState, errorCode);
		}
	}
}
