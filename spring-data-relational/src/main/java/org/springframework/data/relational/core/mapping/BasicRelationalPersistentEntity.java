/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.Optional;

import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * SQL-specific {@link RelationalPersistentEntity} implementation that adds SQL-specific meta-data such as the table and
 * schema name.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Bastian Wilhelm
 * @author Mikhail Polivakha
 * @author Kurt Niemi
 */
class BasicRelationalPersistentEntity<T> extends BasicPersistentEntity<T, RelationalPersistentProperty>
		implements RelationalPersistentEntity<T> {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final Lazy<SqlIdentifier> tableName;
	private final @Nullable Expression tableNameExpression;

	private final Lazy<Optional<SqlIdentifier>> schemaName;
	private final @Nullable Expression schemaNameExpression;
	private final ExpressionEvaluator expressionEvaluator;
	private boolean forceQuote = true;

	/**
	 * Creates a new {@link BasicRelationalPersistentEntity} for the given {@link TypeInformation}.
	 *
	 * @param information must not be {@literal null}.
	 */
	BasicRelationalPersistentEntity(TypeInformation<T> information, NamingStrategy namingStrategy,
			ExpressionEvaluator expressionEvaluator) {

		super(information);

		this.expressionEvaluator = expressionEvaluator;

		Lazy<Optional<SqlIdentifier>> defaultSchema = Lazy.of(() -> StringUtils.hasText(namingStrategy.getSchema())
				? Optional.of(createDerivedSqlIdentifier(namingStrategy.getSchema()))
				: Optional.empty());

		if (isAnnotationPresent(Table.class)) {

			Table table = getRequiredAnnotation(Table.class);

			this.tableName = Lazy.of(() -> StringUtils.hasText(table.value()) ? createSqlIdentifier(table.value())
					: createDerivedSqlIdentifier(namingStrategy.getTableName(getType())));
			this.tableNameExpression = detectExpression(table.value());

			this.schemaName = StringUtils.hasText(table.schema())
					? Lazy.of(() -> Optional.of(createSqlIdentifier(table.schema())))
					: defaultSchema;
			this.schemaNameExpression = detectExpression(table.schema());

		} else {

			this.tableName = Lazy.of(() -> createDerivedSqlIdentifier(namingStrategy.getTableName(getType())));
			this.tableNameExpression = null;
			this.schemaName = defaultSchema;
			this.schemaNameExpression = null;
		}
	}

	/**
	 * Returns a SpEL {@link Expression} if the given {@link String} is actually an expression that does not evaluate to a
	 * {@link LiteralExpression} (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	@Nullable
	private static Expression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		Expression expression = PARSER.parseExpression(potentialExpression, ParserContext.TEMPLATE_EXPRESSION);
		return expression instanceof LiteralExpression ? null : expression;
	}

	private SqlIdentifier createSqlIdentifier(String name) {
		return isForceQuote() ? SqlIdentifier.quoted(name) : SqlIdentifier.unquoted(name);
	}

	private SqlIdentifier createDerivedSqlIdentifier(String name) {
		return new DerivedSqlIdentifier(name, isForceQuote());
	}

	public boolean isForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(boolean forceQuote) {
		this.forceQuote = forceQuote;
	}

	@Override
	public SqlIdentifier getTableName() {

		if (tableNameExpression == null) {
			return tableName.get();
		}

		return createSqlIdentifier(expressionEvaluator.evaluate(tableNameExpression));
	}

	@Override
	public SqlIdentifier getQualifiedTableName() {

		SqlIdentifier schema;
		if (schemaNameExpression != null) {
			schema = createSqlIdentifier(expressionEvaluator.evaluate(schemaNameExpression));
		} else {
			schema = schemaName.get().orElse(null);
		}

		if (schema == null) {
			return getTableName();
		}

		if (schemaNameExpression != null) {
			schema = createSqlIdentifier(expressionEvaluator.evaluate(schemaNameExpression));
		}

		return SqlIdentifier.from(schema, getTableName());
	}

	@Override
	public SqlIdentifier getIdColumn() {
		return getRequiredIdProperty().getColumnName();
	}

	@Override
	public String toString() {
		return String.format("BasicRelationalPersistentEntity<%s>", getType());
	}
}
