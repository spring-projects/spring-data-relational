/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;

/**
 * Simple {@link Dialect} that provides unquoted {@link IdentifierProcessing}.
 *
 * @author Mark Paluch
 */
public class UnquotedDialect extends AbstractDialect implements Dialect {

	public static final UnquotedDialect INSTANCE = new UnquotedDialect();

	private UnquotedDialect() {}

	@Override
	public LimitClause limit() {
		return HsqlDbDialect.INSTANCE.limit();
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.create(new Quoting(""), LetterCasing.AS_IS);
	}
}
