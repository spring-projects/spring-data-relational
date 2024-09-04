/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.data.relational.repository.support;

import java.util.regex.Pattern;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * Replaces SpEL expressions based on table names in query strings.
 *
 * @author Jens Schauder
 */
class TableNameQueryPreprocessor {

	private static final String EXPRESSION_PARAMETER = "$1#{";
	private static final String QUOTED_EXPRESSION_PARAMETER = "$1__HASH__{";

	private static final Pattern EXPRESSION_PARAMETER_QUOTING = Pattern.compile("([:?])#\\{");
	private static final Pattern EXPRESSION_PARAMETER_UNQUOTING = Pattern.compile("([:?])__HASH__\\{");

	private final SqlIdentifier tableName;
	private final SqlIdentifier qualifiedTableName;
	private final Dialect dialect;

	public TableNameQueryPreprocessor(RelationalPersistentEntity<?> entity, Dialect dialect) {
		this(entity.getTableName(), entity.getQualifiedTableName(), dialect);
	}

	TableNameQueryPreprocessor(SqlIdentifier tableName, SqlIdentifier qualifiedTableName, Dialect dialect) {

		Assert.notNull(tableName, "TableName must not be null");
		Assert.notNull(qualifiedTableName, "QualifiedTableName must not be null");
		Assert.notNull(dialect, "Dialect must not be null");

		this.tableName = tableName;
		this.qualifiedTableName = qualifiedTableName;
		this.dialect = dialect;
	}

	public String transform(String query) {

		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setVariable("tableName", tableName.toSql(dialect.getIdentifierProcessing()));
		evaluationContext.setVariable("qualifiedTableName", qualifiedTableName.toSql(dialect.getIdentifierProcessing()));

		SpelExpressionParser parser = new SpelExpressionParser();

		query = quoteExpressionsParameter(query);
		Expression expression = parser.parseExpression(query, ParserContext.TEMPLATE_EXPRESSION);

		return unquoteParameterExpressions(expression.getValue(evaluationContext, String.class));
	}

	private static String unquoteParameterExpressions(String result) {
		return EXPRESSION_PARAMETER_UNQUOTING.matcher(result).replaceAll(EXPRESSION_PARAMETER);
	}

	private static String quoteExpressionsParameter(String query) {
		return EXPRESSION_PARAMETER_QUOTING.matcher(query).replaceAll(QUOTED_EXPRESSION_PARAMETER);
	}
}
