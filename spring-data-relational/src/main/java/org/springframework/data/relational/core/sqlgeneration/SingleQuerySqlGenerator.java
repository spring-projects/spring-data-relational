/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.relational.core.sqlgeneration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.lang.Nullable;

/**
 * A {@link SqlGenerator} that creates SQL statements for loading complete aggregates with a single statement.
 *
 * @author Jens Schauder
 * @since 3.2
 */
public class SingleQuerySqlGenerator implements SqlGenerator {

	private final RelationalMappingContext context;
	private final Dialect dialect;
	private final AliasFactory aliases;

	public SingleQuerySqlGenerator(RelationalMappingContext context, AliasFactory aliasFactory, Dialect dialect) {

		this.context = context;
		this.aliases = aliasFactory;
		this.dialect = dialect;
	}

	@Override
	public String findAll(RelationalPersistentEntity<?> aggregate, @Nullable Condition condition) {
		return createSelect(aggregate, condition);
	}

	String createSelect(RelationalPersistentEntity<?> aggregate, @Nullable Condition condition) {

		AggregatePath rootPath = context.getAggregatePath(aggregate);
		QueryMeta queryMeta = createInlineQuery(rootPath, condition);
		InlineQuery rootQuery = queryMeta.inlineQuery;
		List<Expression> columns = new ArrayList<>(queryMeta.selectableExpressions);

		List<Expression> rownumbers = new ArrayList<>();
		rownumbers.add(queryMeta.rowNumber);

		PersistentPropertyPaths<?, RelationalPersistentProperty> entityPaths = context
				.findPersistentPropertyPaths(aggregate.getType(), PersistentProperty::isEntity);
		List<QueryMeta> inlineQueries = createInlineQueries(entityPaths);
		inlineQueries.forEach(qm -> {
			columns.addAll(qm.selectableExpressions);
			rownumbers.add(qm.rowNumber);
		});

		Expression totalRownumber = rownumbers.size() > 1 ? greatest(rownumbers).as("rn")
				: new AliasedExpression(rownumbers.get(0), "rn");
		columns.add(totalRownumber);

		InlineQuery inlineQuery = createMainSelect(columns, rootPath, rootQuery, inlineQueries);
		Expression rootId = just(aliases.getColumnAlias(rootPath.append(aggregate.getRequiredIdProperty())));

		List<Expression> selectList = getSelectList(queryMeta, inlineQueries, rootId);
		Select fullQuery = StatementBuilder.select(selectList).from(inlineQuery).orderBy(rootId, just("rn")).build(false);

		return SqlRenderer.create(new RenderContextFactory(dialect).createRenderContext()).render(fullQuery);
	}

	private static List<Expression> getSelectList(QueryMeta queryMeta, List<QueryMeta> inlineQueries, Expression rootId) {

		List<Expression> expressions = new ArrayList<>(inlineQueries.size() + queryMeta.simpleColumns.size() + 8);

		queryMeta.simpleColumns
				.forEach(e -> expressions.add(filteredColumnExpression(queryMeta.rowNumber.toString(), e.toString())));

		for (QueryMeta meta : inlineQueries) {

			meta.simpleColumns
					.forEach(e -> expressions.add(filteredColumnExpression(meta.rowNumber.toString(), e.toString())));

			if (meta.id != null) {
				expressions.add(meta.id);
			}
			if (meta.key != null) {
				expressions.add(meta.key);
			}
		}

		expressions.add(rootId);
		return expressions;
	}

	private InlineQuery createMainSelect(List<Expression> columns, AggregatePath rootPath, InlineQuery rootQuery,
			List<QueryMeta> inlineQueries) {

		SelectBuilder.SelectJoin select = StatementBuilder.select(columns).from(rootQuery);
		select = applyJoins(rootPath, inlineQueries, select);

		SelectBuilder.BuildSelect buildSelect = applyWhereCondition(inlineQueries, select);
		return InlineQuery.create(buildSelect.build(false), "main");
	}

	/**
	 * Creates inline queries for all entities referenced by the paths passed as an argument.
	 *
	 * @param paths the paths to consider.
	 * @return a {@link Map} that contains all the inline queries indexed by the path to the entity that gets loaded by
	 *         the subquery.
	 */
	private List<QueryMeta> createInlineQueries(PersistentPropertyPaths<?, RelationalPersistentProperty> paths) {

		List<QueryMeta> inlineQueries = new ArrayList<>();

		for (PersistentPropertyPath<? extends RelationalPersistentProperty> ppp : paths) {

			QueryMeta queryMeta = createInlineQuery(context.getAggregatePath(ppp), null);
			inlineQueries.add(queryMeta);
		}
		return inlineQueries;
	}

	/**
	 * Creates a single inline query for the given basePath. The query selects all the columns for the entity plus a
	 * rownumber and a rowcount expression. The first numbers all rows of the subselect sequentially starting from 1. The
	 * rowcount contains the total number of child rows. All selected expressions are globally uniquely aliased and are
	 * referenced by that alias in the rest of the query. This ensures that we don't run into problems with column names
	 * that are not unique across tables and also the generated SQL doesn't contain quotes and funny column names, making
	 * them easier to understand and also potentially shorter.
	 *
	 * @param basePath the path for which to create the inline query.
	 * @param condition a condition that is to be applied to the query. May be {@literal null}.
	 * @return an inline query for the given path.
	 */
	private QueryMeta createInlineQuery(AggregatePath basePath, @Nullable Condition condition) {

		RelationalPersistentEntity<?> entity = basePath.getRequiredLeafEntity();
		Table table = Table.create(entity.getQualifiedTableName());

		List<AggregatePath> paths = getAggregatePaths(basePath, entity);
		List<Expression> columns = new ArrayList<>();

		String rowNumberAlias = aliases.getRowNumberAlias(basePath);
		Expression rownumber = basePath.isRoot() ? new AliasedExpression(SQL.literalOf(1), rowNumberAlias)
				: createRowNumberExpression(basePath, table, rowNumberAlias);
		columns.add(rownumber);

		String rowCountAlias = aliases.getRowCountAlias(basePath);
		Expression count = basePath.isRoot() ? new AliasedExpression(SQL.literalOf(1), rowCountAlias)
				: AnalyticFunction.create("count", Expressions.just("*"))
						.partitionBy(table.column(basePath.getTableInfo().reverseColumnInfo().name())).as(rowCountAlias);
		columns.add(count);

		String backReferenceAlias = null;
		String keyAlias = null;

		if (!basePath.isRoot()) {

			backReferenceAlias = aliases.getBackReferenceAlias(basePath);
			columns.add(table.column(basePath.getTableInfo().reverseColumnInfo().name()).as(backReferenceAlias));

			keyAlias = aliases.getKeyAlias(basePath);
			Expression keyExpression = basePath.isQualified()
					? table.column(basePath.getTableInfo().qualifierColumnInfo().name()).as(keyAlias)
					: createRowNumberExpression(basePath, table, keyAlias);
			columns.add(keyExpression);
		}

		String id = getIdentifierProperty(paths);
		List<Expression> columnAliases = getColumnAliases(table, paths, columns);
		SelectBuilder.SelectWhere select = StatementBuilder.select(columns).from(table);
		SelectBuilder.BuildSelect buildSelect = condition != null ? select.where(condition) : select;

		InlineQuery inlineQuery = InlineQuery.create(buildSelect.build(false), aliases.getTableAlias(basePath));
		return QueryMeta.of(basePath, inlineQuery, columnAliases, just(id), just(backReferenceAlias), just(keyAlias),
				just(rowNumberAlias), just(rowCountAlias));
	}

	private List<Expression> getColumnAliases(Table table, List<AggregatePath> paths, List<Expression> columns) {

		List<Expression> columnAliases = new ArrayList<>();
		for (AggregatePath path : paths) {

			String alias = aliases.getColumnAlias(path);
			if (!path.getRequiredLeafProperty().isIdProperty()) {
				columnAliases.add(just(alias));
			}
			columns.add(table.column(path.getColumnInfo().name()).as(alias));
		}
		return columnAliases;
	}

	private static List<AggregatePath> getAggregatePaths(AggregatePath basePath, RelationalPersistentEntity<?> entity) {

		List<AggregatePath> paths = new ArrayList<>();

		for (RelationalPersistentProperty property : entity) {
			if (!property.isEntity()) {
				paths.add(basePath.append(property));
			}
		}

		return paths;
	}

	@Nullable
	private String getIdentifierProperty(List<AggregatePath> paths) {

		for (AggregatePath path : paths) {
			if (path.getRequiredLeafProperty().isIdProperty()) {
				return aliases.getColumnAlias(path);
			}
		}

		return null;
	}

	private static AnalyticFunction createRowNumberExpression(AggregatePath basePath, Table table,
			String rowNumberAlias) {
		return AnalyticFunction.create("row_number") //
				.partitionBy(table.column(basePath.getTableInfo().reverseColumnInfo().name())) //
				.orderBy(table.column(basePath.getTableInfo().reverseColumnInfo().name())) //
				.as(rowNumberAlias);
	}

	/**
	 * Adds joins to a select.
	 *
	 * @param rootPath the AggregatePath that gets selected by the select in question.
	 * @param inlineQueries all the inline queries to added as joins as returned by
	 *          {@link #createInlineQueries(PersistentPropertyPaths)}
	 * @param select the select to modify.
	 * @return the original select but with added joins
	 */
	private SelectBuilder.SelectJoin applyJoins(AggregatePath rootPath, List<QueryMeta> inlineQueries,
			SelectBuilder.SelectJoin select) {

		RelationalPersistentProperty rootIdProperty = rootPath.getRequiredIdProperty();
		AggregatePath rootIdPath = rootPath.append(rootIdProperty);
		for (QueryMeta queryMeta : inlineQueries) {

			AggregatePath path = queryMeta.basePath();
			String backReferenceAlias = aliases.getBackReferenceAlias(path);
			Comparison joinCondition = Conditions.isEqual(Expressions.just(aliases.getColumnAlias(rootIdPath)),
					Expressions.just(backReferenceAlias));
			select = select.leftOuterJoin(queryMeta.inlineQuery).on(joinCondition);
		}

		return select;
	}

	/**
	 * Applies a where condition to the select. The Where condition is constructed such that one root and multiple child
	 * selects are combined such that.
	 * <ol>
	 * <li>all child elements with a given rn become part of a single row. I.e. all child rows with for example rownumber
	 * 3 are contained in a single row</li>
	 * <li>if for a given rownumber no matching element is present for a given child the columns for that child are either
	 * null (when there is no child elements at all) or the values for rownumber 1 are used for that child</li>
	 * </ol>
	 *
	 * @param inlineQueries all in the inline queries for all the children, as returned by
	 *          {@link #createInlineQueries(PersistentPropertyPaths)}
	 * @param select the select to which the where clause gets added.
	 * @return the modified select.
	 */
	private SelectBuilder.SelectOrdered applyWhereCondition(List<QueryMeta> inlineQueries,
			SelectBuilder.SelectJoin select) {

		SelectBuilder.SelectWhere selectWhere = (SelectBuilder.SelectWhere) select;

		if (inlineQueries.isEmpty()) {
			return selectWhere;
		}

		Condition joins = null;

		for (int left = 0; left < inlineQueries.size(); left++) {

			QueryMeta leftQueryMeta = inlineQueries.get(left);
			AggregatePath leftPath = leftQueryMeta.basePath;
			Expression leftRowNumber = just(aliases.getRowNumberAlias(leftPath));
			Expression leftRowCount = just(aliases.getRowCountAlias(leftPath));

			for (int right = left + 1; right < inlineQueries.size(); right++) {

				QueryMeta rightQueryMeta = inlineQueries.get(right);
				AggregatePath rightPath = rightQueryMeta.basePath;
				Expression rightRowNumber = just(aliases.getRowNumberAlias(rightPath));
				Expression rightRowCount = just(aliases.getRowCountAlias(rightPath));

				Condition mutualJoin = Conditions.isEqual(leftRowNumber, rightRowNumber).or(Conditions.isNull(leftRowNumber))
						.or(Conditions.isNull(rightRowNumber))
						.or(Conditions.nest(Conditions.isGreater(leftRowNumber, rightRowCount)
								.and(Conditions.isEqual(rightRowNumber, SQL.literalOf(1)))))
						.or(Conditions.nest(Conditions.isGreater(rightRowNumber, leftRowCount)
								.and(Conditions.isEqual(leftRowNumber, SQL.literalOf(1)))));

				mutualJoin = Conditions.nest(mutualJoin);

				if (joins == null) {
					joins = mutualJoin;
				} else {
					joins = joins.and(mutualJoin);
				}
			}
		}

		return selectWhere.where(joins);
	}

	@Override
	public AliasFactory getAliasFactory() {
		return aliases;
	}

	/**
	 * Constructs SQL of the form {@code CASE WHEN x = rn THEN alias ELSE NULL END AS ALIAS}. This expression is used to
	 * replace values that would appear multiple times in the result with {@code null} values in all but the first
	 * occurrence. Without this the result for an aggregate root with a single collection item would look like this:
	 * <table>
	 * <th>
	 * <td>root value</td>
	 * <td>child value</td></th>
	 * <tr>
	 * <td>root1</td>
	 * <td>child1</td>
	 * </tr>
	 * <tr>
	 * <td>root1</td>
	 * <td>child2</td>
	 * </tr>
	 * <tr>
	 * <td>root1</td>
	 * <td>child3</td>
	 * </tr>
	 * <tr>
	 * <td>root1</td>
	 * <td>child4</td>
	 * </tr>
	 * </table>
	 * This expression transforms this into
	 * <table>
	 * <th>
	 * <td>root value</td>
	 * <td>child value</td></th>
	 * <tr>
	 * <td>root1</td>
	 * <td>child1</td>
	 * </tr>
	 * <tr>
	 * <td>null</td>
	 * <td>child2</td>
	 * </tr>
	 * <tr>
	 * <td>null</td>
	 * <td>child3</td>
	 * </tr>
	 * <tr>
	 * <td>null</td>
	 * <td>child4</td>
	 * </tr>
	 * </table>
	 *
	 * @param rowNumberAlias the alias of the rownumber column of the subselect under consideration. This determines if
	 *          the other value is replaced by null or not.
	 * @param alias the column potentially to be replaced by null
	 * @return a SQL expression.
	 */
	private static Expression filteredColumnExpression(String rowNumberAlias, String alias) {
		return just(String.format("case when %s = rn THEN %s else null end as %s", rowNumberAlias, alias, alias));
	}

	private static Expression just(String alias) {
		if (alias == null) {
			return null;
		}
		return Expressions.just(alias);
	}

	/**
	 * Constructs a SQL function of the following form
	 * {@code GREATEST(Coalesce(x1, 1), Coalesce(x2, 1), ..., Coalesce(xN, 1)}. this is used for cobining rownumbers from
	 * different child tables. The {@code coalesce} is used because the values {@code x1 ... xN} might be {@code null} and
	 * we want {@code null} to be equivalent with the first entry.
	 *
	 * @param expressions the different values to combined.
	 */
	private static SimpleFunction greatest(List<Expression> expressions) {

		List<Expression> guarded = new ArrayList<>();
		for (Expression expression : expressions) {
			guarded.add(Functions.coalesce(expression, SQL.literalOf(1)));
		}
		return Functions.greatest(guarded);
	}

	record QueryMeta(AggregatePath basePath, InlineQuery inlineQuery, Collection<Expression> simpleColumns,
			Collection<Expression> selectableExpressions, Expression id, Expression backReference, Expression key,
			Expression rowNumber, Expression rowCount) {

		static QueryMeta of(AggregatePath basePath, InlineQuery inlineQuery, Collection<Expression> simpleColumns,
				Expression id, Expression backReference, Expression key, Expression rowNumber, Expression rowCount) {

			List<Expression> selectableExpressions = new ArrayList<>(simpleColumns);
			selectableExpressions.add(rowNumber);

			if (id != null) {
				selectableExpressions.add(id);
			}
			if (backReference != null) {
				selectableExpressions.add(backReference);
			}
			if (key != null) {
				selectableExpressions.add(key);
			}

			return new QueryMeta(basePath, inlineQuery, simpleColumns, selectableExpressions, id, backReference, key,
					rowNumber, rowCount);
		}
	}
}
