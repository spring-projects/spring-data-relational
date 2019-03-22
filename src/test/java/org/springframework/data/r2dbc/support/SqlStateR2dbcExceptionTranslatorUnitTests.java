/*
 * Copyright 2018-2019 the original author or authors.
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

import org.junit.Test;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.BadSqlGrammarException;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;

/**
 * Unit tests for {@link SqlStateR2dbcExceptionTranslator}.
 *
 * @author Mark Paluch
 */
public class SqlStateR2dbcExceptionTranslatorUnitTests {

	private static final String REASON = "The game is afoot!";
	private static final String TASK = "Counting sheep... yawn.";
	private static final String SQL = "select count(0) from t_sheep where over_fence = ... yawn... 1";

	@Test(expected = IllegalArgumentException.class)
	public void testTranslateNullException() {
		new SqlStateR2dbcExceptionTranslator().translate("", "", null);
	}

	@Test
	public void testTranslateBadSqlGrammar() {
		doTest("07", BadSqlGrammarException.class);
	}

	@Test
	public void testTranslateDataIntegrityViolation() {
		doTest("23", DataIntegrityViolationException.class);
	}

	@Test
	public void testTranslateDataAccessResourceFailure() {
		doTest("53", DataAccessResourceFailureException.class);
	}

	@Test
	public void testTranslateTransientDataAccessResourceFailure() {
		doTest("S1", TransientDataAccessResourceException.class);
	}

	@Test
	public void testTranslateConcurrencyFailure() {
		doTest("40", ConcurrencyFailureException.class);
	}

	@Test
	public void testTranslateUncategorized() {
		doTest("00000000", UncategorizedR2dbcException.class);
	}

	private static void doTest(String sqlState, Class<?> dataAccessExceptionType) {

		R2dbcException ex = new R2dbcException(REASON, sqlState) {};
		SqlStateR2dbcExceptionTranslator translator = new SqlStateR2dbcExceptionTranslator();
		DataAccessException dax = translator.translate(TASK, SQL, ex);

		assertThat(dax).isNotNull().isInstanceOf(dataAccessExceptionType).hasCause(ex);
	}
}
