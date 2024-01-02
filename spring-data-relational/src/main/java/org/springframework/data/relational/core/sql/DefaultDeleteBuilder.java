/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link SelectBuilder} implementation.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DefaultDeleteBuilder implements DeleteBuilder, DeleteBuilder.DeleteWhereAndOr, DeleteBuilder.DeleteWhere {

	private @Nullable Table from;
	private @Nullable Condition where;

	@Override
	public DeleteWhere from(Table table) {

		Assert.notNull(table, "Table must not be null");

		this.from = table;
		return this;
	}

	@Override
	public DeleteWhereAndOr where(Condition condition) {

		Assert.notNull(condition, "Where Condition must not be null");
		this.where = condition;
		return this;
	}

	@Override
	public DeleteWhereAndOr and(Condition condition) {

		Assert.notNull(condition, "Condition must not be null");
		this.where = this.where.and(condition);
		return this;
	}

	@Override
	public DeleteWhereAndOr or(Condition condition) {

		Assert.notNull(condition, "Condition must not be null");
		this.where = this.where.or(condition);
		return this;
	}

	@Override
	public Delete build() {

		DefaultDelete delete = new DefaultDelete(this.from, this.where);

		DeleteValidator.validate(delete);

		return delete;
	}
}
