/*
 * Copyright 2019-2020 the original author or authors.
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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientResourceException;

import org.junit.jupiter.api.Test;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.BadSqlGrammarException;
import org.springframework.data.r2dbc.UncategorizedR2dbcException;

/**
 * Unit tests for {@link R2dbcExceptionSubclassTranslator}.
 *
 * @author Mark Paluch
 */
class R2dbcExceptionSubclassTranslatorUnitTests {

	private final R2dbcExceptionSubclassTranslator translator = new R2dbcExceptionSubclassTranslator();

	@Test // gh-57
	void shouldTranslateTransientResourceException() {

		Exception exception = translator.translate("", "", new R2dbcTransientResourceException(""));

		assertThat(exception)
				.isInstanceOf(TransientDataAccessResourceException.class);
	}

	@Test // gh-57
	void shouldTranslateRollbackException() {

		Exception exception = translator.translate("", "", new R2dbcRollbackException());

		assertThat(exception).isInstanceOf(ConcurrencyFailureException.class);
	}

	@Test // gh-57
	void shouldTranslateTimeoutException() {

		Exception exception = translator.translate("", "", new R2dbcTimeoutException());

		assertThat(exception).isInstanceOf(QueryTimeoutException.class);
	}

	@Test // gh-57
	void shouldNotTranslateUnknownExceptions() {

		Exception exception = translator.translate("", "", new MyTransientExceptions());

		assertThat(exception).isInstanceOf(UncategorizedR2dbcException.class);
	}

	@Test // gh-57
	void shouldTranslateNonTransientResourceException() {

		Exception exception = translator.translate("", "", new R2dbcNonTransientResourceException());

		assertThat(exception).isInstanceOf(DataAccessResourceFailureException.class);
	}

	@Test // gh-57
	void shouldTranslateIntegrityViolationException() {

		Exception exception = translator.translate("", "", new R2dbcDataIntegrityViolationException());

		assertThat(exception).isInstanceOf(DataIntegrityViolationException.class);
	}

	@Test // gh-57
	void shouldTranslatePermissionDeniedException() {

		Exception exception = translator.translate("", "", new R2dbcPermissionDeniedException());

		assertThat(exception).isInstanceOf(PermissionDeniedDataAccessException.class);
	}

	@Test // gh-57
	void shouldTranslateBadSqlGrammarException() {

		Exception exception = translator.translate("", "", new R2dbcBadGrammarException());

		assertThat(exception).isInstanceOf(BadSqlGrammarException.class);
	}

	@Test // gh-57
	void messageGeneration() {

		Exception exception = translator.translate("TASK", "SOME-SQL", new R2dbcTransientResourceException("MESSAGE"));

		assertThat(exception) //
				.isInstanceOf(TransientDataAccessResourceException.class) //
				.hasMessage("TASK; SQL [SOME-SQL]; MESSAGE; nested exception is io.r2dbc.spi.R2dbcTransientResourceException: MESSAGE");
	}

	@Test // gh-57
	void messageGenerationNullSQL() {

		Exception exception = translator.translate("TASK", null, new R2dbcTransientResourceException("MESSAGE"));

		assertThat(exception) //
				.isInstanceOf(TransientDataAccessResourceException.class) //
				.hasMessage("TASK; MESSAGE; nested exception is io.r2dbc.spi.R2dbcTransientResourceException: MESSAGE");
	}

	@Test // gh-57
	void messageGenerationNullMessage() {

		Exception exception = translator.translate("TASK", "SOME-SQL", new R2dbcTransientResourceException());

		assertThat(exception) //
				.isInstanceOf(TransientDataAccessResourceException.class) //
				.hasMessage("TASK; SQL [SOME-SQL]; null; nested exception is io.r2dbc.spi.R2dbcTransientResourceException");
	}

	private static class MyTransientExceptions extends R2dbcException {}
}
