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

/**
 * {@link Dialect} adapter that delegates to the given {@link IdentifierProcessing}.
 *
 * @author Mark Paluch
 */
public class IdentifierProcessingAdapter extends AbstractDialect implements Dialect {

	private final IdentifierProcessing identifierProcessing;

	public IdentifierProcessingAdapter(IdentifierProcessing identifierProcessing) {
		this.identifierProcessing = identifierProcessing;
	}

	@Override
	public LimitClause limit() {
		return HsqlDbDialect.INSTANCE.limit();
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return identifierProcessing;
	}
}
