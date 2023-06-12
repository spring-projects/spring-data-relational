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

import org.jetbrains.annotations.NotNull;
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

/**
 * A {@link SqlGenerator} that creates SQL statements for loading complete aggregates with a single statement.
 * 
 * @since 3.2
 * @author Jens Schauder
 */
public class SingleQuerySqlGenerator implements SqlGenerator {

	private final RelationalMappingContext context;
	private final Dialect dialect;
	private final AliasFactory aliases = new AliasFactory();

	private final RelationalPersistentEntity<?> aggregate;
	private final Table table;

	public SingleQuerySqlGenerator(RelationalMappingContext context, Dialect dialect,
			RelationalPersistentEntity<?> aggregate) {

		this.context = context;
		this.dialect = dialect;
		this.aggregate = aggregate;

		this.table = Table.create(aggregate.getQualifiedTableName());
	}

	@Override
	public String findAll() {
		return createSelect(null);
	}

	@Override
	public String findById() {

		AggregatePath path = getRootIdPath();
		Condition condition = Conditions.isEqual(table.column(path.getColumnInfo().name()), Expressions.just(":id"));

		return createSelect(condition);
	}

	@Override
	public String findAllById() {

		AggregatePath path = getRootIdPath();
		Condition condition = Conditions.in(table.column(path.getColumnInfo().name()), Expressions.just(":ids"));

		return createSelect(condition);
	}

	/**
	 * @return The {@link AggregatePath} to the id property of the aggregate root.
	 */
	private AggregatePath getRootIdPath() {
		return context.getAggregatePath(aggregate).append(aggregate.getRequiredIdProperty());
	}

	/**
	 * Creates a SQL suitable of loading all the data required for constructing complete aggregates.
	 * 
	 * @param condition a constraint for limiting the aggregates to be loaded.
	 * @return a {@literal  String} containing the generated SQL statement
	 */
	private String createSelect(Condition condition) {

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

		Expression rootIdExpression = just(aliases.getColumnAlias(rootPath.append(aggregate.getRequiredIdProperty())));

		List<Expression> finalColumns = new ArrayList<>();
		queryMeta.simpleColumns
				.forEach(e -> finalColumns.add(filteredColumnExpression(queryMeta.rowNumber.toString(), e.toString())));

		for (QueryMeta meta : inlineQueries) {
			meta.simpleColumns
					.forEach(e -> finalColumns.add(filteredColumnExpression(meta.rowNumber.toString(), e.toString())));
			if (meta.id != null) {
				finalColumns.add(meta.id);
			}
			if (meta.key != null) {
				finalColumns.add(meta.key);
			}
		}

		finalColumns.add(rootIdExpression);

		Select fullQuery = StatementBuilder.select(finalColumns).from(inlineQuery).orderBy(rootIdExpression, just("rn"))
				.build();

		return SqlRenderer.create(new RenderContextFactory(dialect).createRenderContext()).render(fullQuery);
	}

	@NotNull
	private InlineQuery createMainSelect(List<Expression> columns, AggregatePath rootPath, InlineQuery rootQuery,
			List<QueryMeta> inlineQueries) {

		SelectBuilder.SelectJoin select = StatementBuilder.select(columns).from(rootQuery);

		select = applyJoins(rootPath, inlineQueries, select);

		SelectBuilder.BuildSelect buildSelect = applyWhereCondition(rootPath, inlineQueries, select);
		Select mainSelect = buildSelect.build();

		return InlineQuery.create(mainSelect, "main");
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

		for (PersistentPropertyPath ppp : paths) {

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
	private QueryMeta createInlineQuery(AggregatePath basePath, Condition condition) {

		RelationalPersistentEntity<?> entity = basePath.getRequiredLeafEntity();
		Table table = Table.create(entity.getQualifiedTableName());

		List<AggregatePath> paths = new ArrayList<>();

		entity.doWithProperties((RelationalPersistentProperty p) -> {
			if (!p.isEntity()) {
				paths.add(basePath.append(p));
			}
		});

		List<Expression> columns = new ArrayList<>();
		List<Expression> columnAliases = new ArrayList<>();

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

			if (basePath.isQualified()) {

				keyAlias = aliases.getKeyAlias(basePath);
				columns.add(table.column(basePath.getTableInfo().qualifierColumnInfo().name()).as(keyAlias));
			} else {

				String alias = aliases.getColumnAlias(basePath);
				columns.add(new AliasedExpression(just("1"), alias));
				columnAliases.add(just(alias));
			}
		}
		String id = null;

		for (AggregatePath path : paths) {

			String alias = aliases.getColumnAlias(path);
			if (path.getRequiredLeafProperty().isIdProperty()) {
				id = alias;
			} else {
				columnAliases.add(just(alias));
			}
			columns.add(table.column(path.getColumnInfo().name()).as(alias));
		}

		SelectBuilder.SelectWhere select = StatementBuilder.select(columns).from(table);

		SelectBuilder.BuildSelect buildSelect = condition != null ? select.where(condition) : select;

		InlineQuery inlineQuery = InlineQuery.create(buildSelect.build(),
				aliases.getTableAlias(context.getAggregatePath(entity)));
		return QueryMeta.of(basePath, inlineQuery, columnAliases, just(id), just(backReferenceAlias), just(keyAlias),
				just(rowNumberAlias), just(rowCountAlias));
	}

	@NotNull
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
	 * @param rootPath path to the root entity that gets selected.
	 * @param inlineQueries all in the inline queries for all the children, as returned by
	 *          {@link #createInlineQueries(PersistentPropertyPaths)}
	 * @param select the select to which the where clause gets added.
	 * @return the modified select.
	 */
	private SelectBuilder.SelectOrdered applyWhereCondition(AggregatePath rootPath, List<QueryMeta> inlineQueries,
			SelectBuilder.SelectJoin select) {

		SelectBuilder.SelectWhereAndOr selectWhere = null;
		for (QueryMeta queryMeta : inlineQueries) {

			AggregatePath path = queryMeta.basePath;
			Expression childRowNumber = just(aliases.getRowNumberAlias(path));
			Condition pseudoJoinCondition = Conditions.isNull(childRowNumber)
					.or(Conditions.isEqual(childRowNumber, Expressions.just(aliases.getRowNumberAlias(rootPath))))
					.or(Conditions.isGreater(childRowNumber, Expressions.just(aliases.getRowCountAlias(rootPath))));

			selectWhere = ((SelectBuilder.SelectWhere) select).where(pseudoJoinCondition);
		}

		return selectWhere == null ? (SelectBuilder.SelectOrdered) select : selectWhere;
	}

	@Override
	public AliasFactory getAliasFactory() {
		return aliases;
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

	/**
	 * Constructs SQL of the form {@code CASE WHEN x = rn THEN alias ELSE NULL END AS ALIAS}. This expression is used to
	 * replace values that would appear multiple times in the result with {@code null} values in all but the first
	 * occurrence. With out this the result for an aggregate root with a single collection item would look like this:
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
	 * @param alias the column potentially to be replace by null
	 * @return a SQL expression.
	 */
	private static Expression filteredColumnExpression(String rowNumberAlias, String alias) {
		return just("case when " + rowNumberAlias + " = rn THEN " + alias + " else null end as " + alias);
	}

	private static Expression just(String alias) {
		if (alias == null) {
			return null;
		}
		return Expressions.just(alias);
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
