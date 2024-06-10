/*
 * Copyright 2017-2025 the original author or authors.
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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @author Yoichi Imai
 * @author Bastian Wilhelm
 * @author Oleksandr Kucher
 * @author Mark Paluch
 * @author Tom Hombergs
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Mikhail Polivakha
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @author Hari Ohm Prasath
 * @author Viktor Ardelean
 * @author Kurt Niemi
 */
class SqlGenerator {

	static final SqlIdentifier VERSION_SQL_PARAMETER = SqlIdentifier.unquoted("___oldOptimisticLockingVersion");
	static final SqlIdentifier IDS_SQL_PARAMETER = SqlIdentifier.unquoted("ids");

	/**
	 * Length of an aggregate path that is one longer then the root path.
	 */
	private static final int FIRST_NON_ROOT_LENGTH = 2;

	private final RelationalPersistentEntity<?> entity;
	private final RelationalMappingContext mappingContext;

	private final SqlContext sqlContext;
	private final SqlRenderer sqlRenderer;
	private final Columns columns;

	private final Lazy<String> findOneSql = Lazy.of(this::createFindOneSql);
	private final Lazy<String> findAllSql = Lazy.of(this::createFindAllSql);
	private final Lazy<String> findAllInListSql = Lazy.of(this::createFindAllInListSql);

	private final Lazy<String> existsSql = Lazy.of(this::createExistsSql);
	private final Lazy<String> countSql = Lazy.of(this::createCountSql);

	private final Lazy<String> updateSql = Lazy.of(this::createUpdateSql);
	private final Lazy<String> updateWithVersionSql = Lazy.of(this::createUpdateWithVersionSql);

	private final Lazy<String> deleteByIdSql = Lazy.of(this::createDeleteByIdSql);
	private final Lazy<String> deleteByIdInSql = Lazy.of(this::createDeleteByIdInSql);
	private final Lazy<String> deleteByIdAndVersionSql = Lazy.of(this::createDeleteByIdAndVersionSql);
	private final Lazy<String> deleteByListSql = Lazy.of(this::createDeleteByListSql);
	private final QueryMapper queryMapper;
	private final Dialect dialect;

	private final Function<Map<AggregatePath, Column>, Condition> inCondition;
	private final Function<Map<AggregatePath, Column>, Condition> equalityCondition;
	private final Function<Map<AggregatePath, Column>, Condition> notNullCondition;

	/**
	 * Create a new {@link SqlGenerator} given {@link RelationalMappingContext} and {@link RelationalPersistentEntity}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 */
	SqlGenerator(RelationalMappingContext mappingContext, JdbcConverter converter, RelationalPersistentEntity<?> entity,
			Dialect dialect) {

		this.mappingContext = mappingContext;
		this.entity = entity;
		this.sqlContext = new SqlContext(entity);
		this.sqlRenderer = SqlRenderer.create(new RenderContextFactory(dialect).createRenderContext());
		this.columns = new Columns(entity, mappingContext, converter);
		this.queryMapper = new QueryMapper(converter);
		this.dialect = dialect;

		inCondition = inCondition();
		equalityCondition = equalityCondition();
		notNullCondition = isNotNullCondition();

	}

	/**
	 * When deleting entities there is a fundamental difference between deleting
	 * <ol>
	 * <li>the aggregate root.</li>
	 * <li>a first level entity which still references the root id directly</li>
	 * <li>and all other entities which have to use a subselect to navigate from the id of the aggregate root to something
	 * referenced by the table in question.</li>
	 * </ol>
	 * For paths of the second kind this method returns {@literal true}.
	 *
	 * @param path the path to analyze.
	 * @return If the given path is considered deeply nested.
	 */
	private static boolean isFirstNonRoot(AggregatePath path) {
		return path.getLength() == FIRST_NON_ROOT_LENGTH;
	}

	/**
	 * When deleting entities there is a fundamental difference between deleting
	 * <ol>
	 * <li>the aggregate root.</li>
	 * <li>a first level entity which still references the root id directly</li>
	 * <li>and all other entities which have to use a subselect to navigate from the id of the aggregate root to something
	 * referenced by the table in question.</li>
	 * </ol>
	 * For paths of the third kind this method returns {@literal true}.
	 *
	 * @param path the path to analyze.
	 * @return If the given path is considered deeply nested.
	 */
	private static boolean isDeeplyNested(AggregatePath path) {
		return path.getLength() > FIRST_NON_ROOT_LENGTH;
	}

	/**
	 * Construct an IN-condition based on a {@link Select Sub-Select} which selects the ids (or stand-ins for ids) of the
	 * given {@literal path} to those that reference the root entities specified by the {@literal rootCondition}.
	 *
	 * @param path specifies the table and id to select
	 * @param conditionFunction a function for construction a where clause
	 * @param columns map making all columns available as a map from {@link AggregatePath}
	 * @return the IN condition
	 */
	private Condition getSubselectCondition(AggregatePath path,
			Function<Map<AggregatePath, Column>, Condition> conditionFunction, Map<AggregatePath, Column> columns) {

		AggregatePath parentPath = path.getParentPath();

		if (!parentPath.hasIdProperty()) {
			if (isDeeplyNested(parentPath)) {
				return getSubselectCondition(parentPath, conditionFunction, columns);
			}
			return conditionFunction.apply(columns);
		}

		AggregatePath.TableInfo parentPathTableInfo = parentPath.getTableInfo();
		Table subSelectTable = Table.create(parentPathTableInfo.qualifiedTableName());

		Map<AggregatePath, Column> selectFilterColumns = new TreeMap<>();
		parentPathTableInfo.effectiveIdColumnInfos().forEach( //
				(ap, ci) -> //
				selectFilterColumns.put(ap, subSelectTable.column(ci.name())) //
		);

		Condition innerCondition;

		if (isFirstNonRoot(parentPath)) { // if the parent is the root of the path
			// apply the rootCondition
			innerCondition = conditionFunction.apply(selectFilterColumns);
		} else {
			// otherwise, we need another layer of subselect
			innerCondition = getSubselectCondition(parentPath, conditionFunction, selectFilterColumns);
		}

		List<Expression> idColumns = parentPathTableInfo.idColumnInfos().toList(ci -> subSelectTable.column(ci.name()));

		Select select = Select.builder() //
				.select(idColumns) //
				.from(subSelectTable) //
				.where(innerCondition).build();

		return Conditions.in(toExpression(columns), select);
	}

	private Expression toExpression(Map<AggregatePath, Column> columnsMap) {
			return TupleExpression.maybeWrap(new ArrayList<>(columnsMap.values()));
	}

	private BindMarker getBindMarker(SqlIdentifier columnName) {
		return SQL.bindMarker(":" + BindParameterNameSanitizer.sanitize(renderReference(columnName)));
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 * Results are filtered using an {@code IN}-clause on the id column.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAllInList() {
		return findAllInListSql.get();
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAll() {
		return findAllSql.get();
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships,
	 * sorted by the given parameter.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAll(Sort sort) {
		return render(selectBuilder(Collections.emptyList(), sort, Pageable.unpaged()).build());
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships,
	 * paged and sorted by the given parameter.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAll(Pageable pageable) {
		return render(selectBuilder(Collections.emptyList(), pageable.getSort(), pageable).build());
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 * Results are limited to those rows referencing some parent entity. This is used to select values for a complex
	 * property ({@link Set}, {@link Map} ...) based on a referencing entity.
	 *
	 * @param parentIdentifier name of the column of the FK back to the referencing entity.
	 * @param propertyPath used to determine if the property is ordered and if there is a key column.
	 * @return a SQL String.
	 * @since 3.0
	 */
	String getFindAllByProperty(Identifier parentIdentifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath) {

		Assert.notNull(parentIdentifier, "identifier must not be null");
		Assert.notNull(propertyPath, "propertyPath must not be null");

		AggregatePath path = mappingContext.getAggregatePath(propertyPath);

		return getFindAllByProperty(parentIdentifier, path.getTableInfo().qualifierColumnInfo(), path.isOrdered());
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 * Results are limited to those rows referencing some other entity using the column specified by
	 * {@literal columnName}. This is used to select values for a complex property ({@link Set}, {@link Map} ...) based on
	 * a referencing entity.
	 *
	 * @param parentIdentifier name of the column of the FK back to the referencing entity.
	 * @param keyColumn if the property is of type {@link Map} this column contains the map key.
	 * @param ordered whether the SQL statement should include an ORDER BY for the keyColumn. If this is {@code true}, the
	 *          keyColumn must not be {@code null}.
	 * @return a SQL String.
	 */
	String getFindAllByProperty(Identifier parentIdentifier, @Nullable AggregatePath.ColumnInfo keyColumn,
			boolean ordered) {

		Assert.isTrue(keyColumn != null || !ordered,
				"If the SQL statement should be ordered a keyColumn to order by must be provided");

		Table table = getTable();

		SelectBuilder.SelectWhere builder = selectBuilder( //
				keyColumn == null //
						? Collections.emptyList() //
						: Collections.singleton(keyColumn.name()) //
		);

		Condition condition = buildConditionForBackReference(parentIdentifier, table);
		SelectBuilder.SelectWhereAndOr withWhereClause = builder.where(condition);

		Select select = ordered //
				? withWhereClause.orderBy(table.column(keyColumn.name()).as(keyColumn.alias())).build() //
				: withWhereClause.build();

		return render(select);
	}

	private Condition buildConditionForBackReference(Identifier parentIdentifier, Table table) {

		Condition condition = null;
		for (SqlIdentifier backReferenceColumn : parentIdentifier.toMap().keySet()) {

			Assert.isTrue(!SqlIdentifier.EMPTY.equals(backReferenceColumn),
					"An empty SqlIdentifier can't be used in condition. Make sure that all composite primary keys are defined in the query");

			Condition newCondition = table.column(backReferenceColumn).isEqualTo(getBindMarker(backReferenceColumn));
			condition = condition == null ? newCondition : condition.and(newCondition);
		}

		Assert.state(condition != null, "We need at least one condition");

		return condition;
	}

	/**
	 * Create a {@code SELECT COUNT(id) FROM … WHERE :id = …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getExists() {
		return existsSql.get();
	}

	/**
	 * Create a {@code SELECT … FROM … WHERE :id = …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getFindOne() {
		return findOneSql.get();
	}

	/**
	 * Create a {@code SELECT count(id) FROM … WHERE :id = … (LOCK CLAUSE)} statement.
	 *
	 * @param lockMode Lock clause mode.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getAcquireLockById(LockMode lockMode) {
		return this.createAcquireLockById(lockMode);
	}

	/**
	 * Create a {@code SELECT count(id) FROM … (LOCK CLAUSE)} statement.
	 *
	 * @param lockMode Lock clause mode.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getAcquireLockAll(LockMode lockMode) {
		return this.createAcquireLockAll(lockMode);
	}

	/**
	 * Create a {@code INSERT INTO … (…) VALUES(…)} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getInsert(Set<SqlIdentifier> additionalColumns) {
		return createInsertSql(additionalColumns);
	}

	/**
	 * Create a {@code UPDATE … SET …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getUpdate() {
		return updateSql.get();
	}

	/**
	 * Create a {@code UPDATE … SET … WHERE ID = :id and VERSION_COLUMN = :___oldOptimisticLockingVersion } statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getUpdateWithVersion() {
		return updateWithVersionSql.get();
	}

	/**
	 * Create a {@code SELECT COUNT(*) FROM …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getCount() {
		return countSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :id = …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getDeleteById() {
		return deleteByIdSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :id IN …} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getDeleteByIdIn() {
		return deleteByIdInSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :id = … and :___oldOptimisticLockingVersion = ...} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getDeleteByIdAndVersion() {
		return deleteByIdAndVersionSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :ids in (…)} statement.
	 *
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String getDeleteByList() {
		return deleteByListSql.get();
	}

	/**
	 * Create a {@code DELETE} query and optionally filter by {@link PersistentPropertyPath}.
	 *
	 * @param path can be {@literal null}.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String createDeleteAllSql(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		Table table = getTable();

		DeleteBuilder.DeleteWhere deleteAll = Delete.builder().from(table);

		if (path == null) {
			return render(deleteAll.build());
		}

		return createDeleteByPathAndCriteria(mappingContext.getAggregatePath(path), notNullCondition);
	}

	/**
	 * Create a {@code DELETE} query and filter by {@link PersistentPropertyPath} using {@code WHERE} with the {@code =}
	 * operator.
	 *
	 * @param path must not be {@literal null}.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String createDeleteByPath(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return createDeleteByPathAndCriteria(mappingContext.getAggregatePath(path), equalityCondition);
	}

	/**
	 * Create a {@code DELETE} query and filter by {@link PersistentPropertyPath} using {@code WHERE} with the {@code IN}
	 * operator.
	 *
	 * @param path must not be {@literal null}.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	String createDeleteInByPath(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return createDeleteByPathAndCriteria(mappingContext.getAggregatePath(path), inCondition);
	}

	/**
	 * Constructs a function for constructing a where condition. The where condition will be of the form
	 * {@literal <columns> IN :bind-marker}
	 */
	private Function<Map<AggregatePath, Column>, Condition> inCondition() {

		return columnMap -> {

			List<Column> columns = List.copyOf(columnMap.values());

			if (columns.size() == 1) {
				return Conditions.in(columns.get(0), getBindMarker(IDS_SQL_PARAMETER));
			}
			return Conditions.in(TupleExpression.create(columns), getBindMarker(IDS_SQL_PARAMETER));
		};
	}

	/**
	 * Constructs a function for constructing a where. The where condition will be of the form
	 * {@literal <column-a> = :bind-marker-a AND <column-b> = :bind-marker-b ...}
	 */
	private Function<Map<AggregatePath, Column>, Condition> equalityCondition() {

		AggregatePath.ColumnInfos idColumnInfos = mappingContext.getAggregatePath(entity).getTableInfo().idColumnInfos();

		return columnMap -> {

			Condition result = null;
			for (Map.Entry<AggregatePath, Column> entry : columnMap.entrySet()) {
				BindMarker bindMarker = getBindMarker(idColumnInfos.get(entry.getKey()).name());
				Comparison singleCondition = entry.getValue().isEqualTo(bindMarker);

				result = result == null ? singleCondition : result.and(singleCondition);
			}
			return result;
		};
	}

	/**
	 * Constructs a function for constructing where a condition. The where condition will be of the form
	 * {@literal <column-a> IS NOT NULL AND <column-b> IS NOT NULL ... }
	 */
	private Function<Map<AggregatePath, Column>, Condition> isNotNullCondition() {

		return columnMap -> {

			Condition result = null;
			for (Column column : columnMap.values()) {
				Condition singleCondition = column.isNotNull();

				result = result == null ? singleCondition : result.and(singleCondition);
			}
			return result;
		};
	}

	private String createFindOneSql() {

		return render(selectBuilder().where(equalityIdWhereCondition()).build());
	}

	private Condition equalityIdWhereCondition() {

		Condition aggregate = null;
		for (Column column : getIdColumns()) {

			Comparison condition = column.isEqualTo(getBindMarker(column.getName()));
			aggregate = aggregate == null ? condition : aggregate.and(condition);
		}

		Assert.state(aggregate != null, "We need at least one id column");

		return aggregate;
	}

	private String createAcquireLockById(LockMode lockMode) {

		Table table = this.getTable();

		Select select = StatementBuilder //
				.select(getSingleNonNullColumn()) //
				.from(table) //
				.where(equalityIdWhereCondition()) //
				.lock(lockMode) //
				.build();

		return render(select);
	}

	private String createAcquireLockAll(LockMode lockMode) {

		Table table = this.getTable();

		Select select = StatementBuilder //
				.select(getSingleNonNullColumn()) //
				.from(table) //
				.lock(lockMode) //
				.build();

		return render(select);
	}

	private String createFindAllSql() {
		return render(selectBuilder().build());
	}

	private SelectBuilder.SelectWhere selectBuilder() {
		return selectBuilder(Collections.emptyList());
	}

	private SelectBuilder.SelectWhere selectBuilder(Collection<SqlIdentifier> keyColumns) {

		Table table = getTable();

		Set<Expression> columnExpressions = new LinkedHashSet<>();

		List<Join> joinTables = new ArrayList<>();
		for (PersistentPropertyPath<RelationalPersistentProperty> path : mappingContext
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			AggregatePath extPath = mappingContext.getAggregatePath(path);

			// add a join if necessary
			Join join = getJoin(extPath);
			if (join != null) {
				joinTables.add(join);
			}

			Column column = getColumn(extPath);
			if (column != null) {
				columnExpressions.add(column);
			}
		}

		for (SqlIdentifier keyColumn : keyColumns) {
			columnExpressions.add(table.column(keyColumn).as(keyColumn));
		}

		SelectBuilder.SelectJoin baseSelect = StatementBuilder.select(columnExpressions).from(table);

		return (SelectBuilder.SelectWhere) addJoins(baseSelect, joinTables);
	}

	private static SelectBuilder.SelectJoin addJoins(SelectBuilder.SelectJoin baseSelect, List<Join> joinTables) {

		for (Join join : joinTables) {

			Condition condition = null;
			for (Pair<Column, Column> columnPair : join.columns) {
				Comparison elementalCondition = columnPair.getFirst().isEqualTo(columnPair.getSecond());
				condition = condition == null ? elementalCondition : condition.and(elementalCondition);
			}

			baseSelect = baseSelect.leftOuterJoin(join.joinTable).on(Objects.requireNonNull(condition));
		}
		return baseSelect;
	}

	private SelectBuilder.SelectOrdered selectBuilder(Collection<SqlIdentifier> keyColumns, Sort sort,
			Pageable pageable) {

		SelectBuilder.SelectOrdered sortable = this.selectBuilder(keyColumns);
		sortable = applyPagination(pageable, sortable);
		return sortable.orderBy(extractOrderByFields(sort));

	}

	private SelectBuilder.SelectOrdered applyPagination(Pageable pageable, SelectBuilder.SelectOrdered select) {

		if (!pageable.isPaged()) {
			return select;
		}

		Assert.isTrue(select instanceof SelectBuilder.SelectLimitOffset,
				() -> String.format("Can't apply limit clause to statement of type %s", select.getClass()));

		SelectBuilder.SelectLimitOffset limitable = (SelectBuilder.SelectLimitOffset) select;
		SelectBuilder.SelectLimitOffset limitResult = limitable.limitOffset(pageable.getPageSize(), pageable.getOffset());

		Assert.state(limitResult instanceof SelectBuilder.SelectOrdered, String.format(
				"The result of applying the limit-clause must be of type SelectOrdered in order to apply the order-by-clause but is of type %s",
				select.getClass()));

		return (SelectBuilder.SelectOrdered) limitResult;
	}

	/**
	 * Create a {@link Column} for {@link AggregatePath}.
	 *
	 * @param path the path to the column in question.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	@Nullable
	Column getColumn(AggregatePath path) {

		// an embedded itself doesn't give a column, its members will though.
		// if there is a collection or map on the path it won't get selected at all, but it will get loaded with a separate
		// select
		// only the parent path is considered in order to handle arrays that get stored as BINARY properly
		if (path.isEmbedded() || path.getParentPath().isMultiValued()) {
			return null;
		}

		if (path.isEntity()) {

			// Simple entities without id include there backreference as a synthetic id in order to distinguish null entities
			// from entities with only null values.

			if (path.isQualified() //
					|| path.isCollectionLike() //
					|| path.hasIdProperty() //
			) {
				return null;
			}

			return sqlContext.getAnyReverseColumn(path);
		}

		return sqlContext.getColumn(path);
	}

	@Nullable
	Join getJoin(AggregatePath path) {

		if (!path.isEntity() || path.isEmbedded() || path.isMultiValued()) {
			return null;
		}

		Table currentTable = sqlContext.getTable(path);
		AggregatePath.ColumnInfos backRefColumnInfos = path.getTableInfo().reverseColumnInfos();

		AggregatePath idDefiningParentPath = path.getIdDefiningParentPath();
		Table parentTable = sqlContext.getTable(idDefiningParentPath);
		AggregatePath.ColumnInfos idColumnInfos = idDefiningParentPath.getTableInfo().idColumnInfos();

		List<Pair<Column, Column>> joinConditions = new ArrayList<>();
		backRefColumnInfos.forEach((ap, ci) -> {
			joinConditions.add(Pair.of(currentTable.column(ci.name()), parentTable.column(idColumnInfos.get(ap).name())));
		});

		return new Join( //
				currentTable, //
				joinConditions //
		);
	}

	private String createFindAllInListSql() {

		In condition = idInWhereClause();
		Select select = selectBuilder().where(condition).build();

		return render(select);
	}

	private In idInWhereClause() {

		List<Column> idColumns = getIdColumns();
		Expression expression = idColumns.size() == 1 ? idColumns.get(0) : TupleExpression.create(idColumns);

		return Conditions.in(expression, getBindMarker(IDS_SQL_PARAMETER));
	}

	private String createExistsSql() {
		Table table = getTable();

		Select select = StatementBuilder //
				.select(Functions.count(getSingleNonNullColumn())) //
				.from(table) //
				.where(equalityIdWhereCondition()) //
				.build();

		return render(select);
	}

	private String createCountSql() {

		Table table = getTable();

		Select select = StatementBuilder //
				.select(Functions.count(Expressions.asterisk())) //
				.from(table) //
				.build();

		return render(select);
	}

	private String createInsertSql(Set<SqlIdentifier> additionalColumns) {

		Table table = getTable();

		Set<SqlIdentifier> columnNamesForInsert = new TreeSet<>(Comparator.comparing(SqlIdentifier::getReference));
		columnNamesForInsert.addAll(columns.getInsertableColumns());
		columnNamesForInsert.addAll(additionalColumns);

		InsertBuilder.InsertIntoColumnsAndValuesWithBuild insert = Insert.builder().into(table);

		for (SqlIdentifier cn : columnNamesForInsert) {
			insert = insert.column(table.column(cn));
		}

		if (columnNamesForInsert.isEmpty()) {
			return render(insert.build());
		}

		InsertBuilder.InsertValuesWithBuild insertWithValues = null;
		for (SqlIdentifier cn : columnNamesForInsert) {
			insertWithValues = (insertWithValues == null ? insert : insertWithValues).values(getBindMarker(cn));
		}

		return render(insertWithValues.build());
	}

	private String createUpdateSql() {
		return render(createBaseUpdate().build());
	}

	private String createUpdateWithVersionSql() {

		Update update = createBaseUpdate() //
				.and(getVersionColumn().isEqualTo(getBindMarker(VERSION_SQL_PARAMETER))) //
				.build();

		return render(update);
	}

	private UpdateBuilder.UpdateWhereAndOr createBaseUpdate() {

		Table table = getTable();

		List<AssignValue> assignments = columns.getUpdatableColumns() //
				.stream() //
				.map(columnName -> Assignments.value( //
						table.column(columnName), //
						getBindMarker(columnName))) //
				.collect(Collectors.toList());

		return Update.builder() //
				.table(table) //
				.set(assignments) //
				.where(equalityIdWhereCondition());
	}

	private String createDeleteByIdSql() {
		return render(createBaseDeleteById(getTable()).build());
	}

	private String createDeleteByIdInSql() {
		return render(createBaseDeleteByIdIn(getTable()).build());
	}

	private String createDeleteByIdAndVersionSql() {

		Delete delete = createBaseDeleteById(getTable()) //
				.and(getVersionColumn().isEqualTo(getBindMarker(VERSION_SQL_PARAMETER))) //
				.build();

		return render(delete);
	}

	private DeleteBuilder.DeleteWhereAndOr createBaseDeleteById(Table table) {

		return Delete.builder().from(table) //
				.where(equalityIdWhereCondition());
	}

	private DeleteBuilder.DeleteWhereAndOr createBaseDeleteByIdIn(Table table) {

		return Delete.builder().from(table) //
				.where(idInWhereClause());
	}

	private String createDeleteByPathAndCriteria(AggregatePath path,
			Function<Map<AggregatePath, Column>, Condition> multiIdCondition) {

		Table table = Table.create(path.getTableInfo().qualifiedTableName());

		DeleteBuilder.DeleteWhere builder = Delete.builder() //
				.from(table);
		Delete delete;

		Map<AggregatePath, Column> columns = new TreeMap<>();
		AggregatePath.ColumnInfos columnInfos = path.getTableInfo().reverseColumnInfos();
		columnInfos.forEach((ag, ci) -> columns.put(ag, table.column(ci.name())));

		if (isFirstNonRoot(path)) {

			delete = builder //
					.where(multiIdCondition.apply(columns)) //
					.build();
		} else {

			Condition condition = getSubselectCondition(path, multiIdCondition, columns);
			delete = builder.where(condition).build();
		}

		return render(delete);
	}

	private String createDeleteByListSql() {

		Table table = getTable();

		Delete delete = Delete.builder() //
				.from(table) //
				.where(idInWhereClause()) //
				.build();

		return render(delete);
	}

	private String render(Select select) {
		return this.sqlRenderer.render(select);
	}

	private String render(Insert insert) {
		return this.sqlRenderer.render(insert);
	}

	private String render(Update update) {
		return this.sqlRenderer.render(update);
	}

	private String render(Delete delete) {
		return this.sqlRenderer.render(delete);
	}

	private Table getTable() {
		return sqlContext.getTable();
	}

	/**
	 * @return a single column of the primary key to be used in places where one need something not null to be selected.
	 */
	private Column getSingleNonNullColumn() {

		AggregatePath.ColumnInfos columnInfos = mappingContext.getAggregatePath(entity).getTableInfo().idColumnInfos();
		return columnInfos.any((ap, ci) -> sqlContext.getTable(columnInfos.fullPath(ap)).column(ci.name()).as(ci.alias()));
	}

	private List<Column> getIdColumns() {

		AggregatePath.ColumnInfos columnInfos = mappingContext.getAggregatePath(entity).getTableInfo().idColumnInfos();
		List<Column> result = new ArrayList<>(columnInfos.size());
		columnInfos.forEach((ap, ci) -> result.add(sqlContext.getColumn(columnInfos.fullPath(ap))));

		return result;
	}

	private Column getVersionColumn() {
		return sqlContext.getVersionColumn();
	}

	private String renderReference(SqlIdentifier identifier) {
		return identifier.getReference();
	}

	private List<OrderByField> extractOrderByFields(Sort sort) {

		return sort.stream() //
				.map(this::orderToOrderByField) //
				.collect(Collectors.toList());
	}

	private OrderByField orderToOrderByField(Sort.Order order) {

		SqlIdentifier columnName = getColumnNameToSortBy(order);
		Column column = Column.create(columnName, this.getTable());
		return OrderByField.from(column, order.getDirection()).withNullHandling(order.getNullHandling());
	}

	private SqlIdentifier getColumnNameToSortBy(Sort.Order order) {

		RelationalPersistentProperty propertyToSortBy = entity.getPersistentProperty(order.getProperty());
		if (propertyToSortBy != null) {
			return propertyToSortBy.getColumnName();
		}

		PersistentPropertyPath<RelationalPersistentProperty> persistentPropertyPath = mappingContext
				.getPersistentPropertyPath(order.getProperty(), entity.getTypeInformation());

		propertyToSortBy = persistentPropertyPath.getBaseProperty();

		Assert.state(propertyToSortBy != null && propertyToSortBy.isEmbedded(), () -> String.format( //
				"Specified sorting property '%s' is expected to " + //
						"be the property, named '%s', of embedded entity '%s', but field '%s' is " + //
						"not marked with @Embedded", //
				order.getProperty(), //
				extractFieldNameFromEmbeddedProperty(order), //
				extractEmbeddedPropertyName(order), //
				extractEmbeddedPropertyName(order) //
		));

		RelationalPersistentEntity<?> embeddedEntity = mappingContext
				.getRequiredPersistentEntity(propertyToSortBy.getType());
		return embeddedEntity.getRequiredPersistentProperty(extractFieldNameFromEmbeddedProperty(order)).getColumnName();
	}

	public String extractEmbeddedPropertyName(Sort.Order order) {
		return order.getProperty().substring(0, order.getProperty().indexOf("."));
	}

	public String extractFieldNameFromEmbeddedProperty(Sort.Order order) {
		return order.getProperty().substring(order.getProperty().indexOf(".") + 1);
	}

	/**
	 * Constructs a single sql query that performs select based on the provided query. Additional the bindings for the
	 * where clause are stored after execution into the <code>parameterSource</code>
	 *
	 * @param query the query to base the select on. Must not be null
	 * @param parameterSource the source for holding the bindings
	 * @return a non null query string.
	 */
	public String selectByQuery(Query query, MapSqlParameterSource parameterSource) {

		Assert.notNull(parameterSource, "parameterSource must not be null");

		SelectBuilder.SelectWhere selectBuilder = selectBuilder();

		Select select = applyQueryOnSelect(query, parameterSource, selectBuilder) //
				.build();

		return render(select);
	}

	/**
	 * Constructs a single sql query that performs select based on the provided query and pagination information.
	 * Additional the bindings for the where clause are stored after execution into the <code>parameterSource</code>
	 *
	 * @param query the query to base the select on. Must not be null.
	 * @param pageable the pageable to perform on the select.
	 * @param parameterSource the source for holding the bindings.
	 * @return a non null query string.
	 */
	public String selectByQuery(Query query, MapSqlParameterSource parameterSource, Pageable pageable) {

		Assert.notNull(parameterSource, "parameterSource must not be null");

		SelectBuilder.SelectWhere selectBuilder = selectBuilder();

		// first apply query and then pagination. This means possible query sorting and limiting might be overwritten by the
		// pagination. This is desired.
		SelectBuilder.SelectOrdered selectOrdered = applyQueryOnSelect(query, parameterSource, selectBuilder);
		selectOrdered = applyPagination(pageable, selectOrdered);
		selectOrdered = selectOrdered.orderBy(extractOrderByFields(pageable.getSort()));

		Select select = selectOrdered.build();
		return render(select);
	}

	/**
	 * Constructs a single sql query that performs select count based on the provided query for checking existence.
	 * Additional the bindings for the where clause are stored after execution into the <code>parameterSource</code>
	 *
	 * @param query the query to base the select on. Must not be null
	 * @param parameterSource the source for holding the bindings
	 * @return a non null query string.
	 */
	public String existsByQuery(Query query, MapSqlParameterSource parameterSource) {

		SelectBuilder.SelectJoin baseSelect = getExistsSelect();

		Select select = applyQueryOnSelect(query, parameterSource, (SelectBuilder.SelectWhere) baseSelect) //
				.build();

		return render(select);
	}

	/**
	 * Constructs a single sql query that performs select count based on the provided query. Additional the bindings for
	 * the where clause are stored after execution into the <code>parameterSource</code>
	 *
	 * @param query the query to base the select on. Must not be null
	 * @param parameterSource the source for holding the bindings
	 * @return a non null query string.
	 */
	public String countByQuery(Query query, MapSqlParameterSource parameterSource) {

		Expression countExpression = Expressions.just("1");
		SelectBuilder.SelectJoin baseSelect = getSelectCountWithExpression(countExpression);

		Select select = applyQueryOnSelect(query, parameterSource, (SelectBuilder.SelectWhere) baseSelect) //
				.build();

		return render(select);
	}

	/**
	 * Generates a {@link org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin} with a
	 * <code>COUNT(...)</code> where the <code>countExpressions</code> are the parameters of the count.
	 *
	 * @return a non-null {@link org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin} that joins all the
	 *         columns and has only a count in the projection of the select.
	 */
	private SelectBuilder.SelectJoin getExistsSelect() {

		Table table = getTable();

		SelectBuilder.SelectJoin baseSelect = StatementBuilder //
				.select(dialect.getExistsFunction()) //
				.from(table);

		// collect joins
		List<Join> joins = new ArrayList<>();
		for (PersistentPropertyPath<RelationalPersistentProperty> path : mappingContext
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			AggregatePath aggregatePath = mappingContext.getAggregatePath(path);

			// add a join if necessary
			Join join = getJoin(aggregatePath);
			if (join != null) {
				joins.add(join);
			}
		}

		return addJoins(baseSelect, joins);
	}

	/**
	 * Generates a {@link org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin} with a
	 * <code>COUNT(...)</code> where the <code>countExpressions</code> are the parameters of the count.
	 *
	 * @param countExpressions the expression to use as count parameter.
	 * @return a non-null {@link org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin} that joins all the
	 *         columns and has only a count in the projection of the select.
	 */
	private SelectBuilder.SelectJoin getSelectCountWithExpression(Expression... countExpressions) {

		Assert.notNull(countExpressions, "countExpressions must not be null");
		Assert.state(countExpressions.length >= 1, "countExpressions must contain at least one expression");

		Table table = getTable();

		SelectBuilder.SelectJoin baseSelect = StatementBuilder //
				.select(Functions.count(countExpressions)) //
				.from(table);

		List<Join> joins = new ArrayList<>();
		// add possible joins
		for (PersistentPropertyPath<RelationalPersistentProperty> path : mappingContext
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			AggregatePath extPath = mappingContext.getAggregatePath(path);

			// add a join if necessary
			Join join = getJoin(extPath);
			if (join != null) {
				joins.add(join);
			}
		}
		return addJoins(baseSelect, joins);
	}

	private SelectBuilder.SelectOrdered applyQueryOnSelect(Query query, MapSqlParameterSource parameterSource,
			SelectBuilder.SelectWhere selectBuilder) {

		Table table = Table.create(this.entity.getQualifiedTableName());

		SelectBuilder.SelectOrdered selectOrdered = query //
				.getCriteria() //
				.map(item -> this.applyCriteria(item, selectBuilder, parameterSource, table)) //
				.orElse(selectBuilder);

		if (query.isSorted()) {
			List<OrderByField> sort = this.queryMapper.getMappedSort(table, query.getSort(), entity);
			selectOrdered = selectOrdered.orderBy(sort);
		}

		SelectBuilder.SelectLimitOffset limitable = (SelectBuilder.SelectLimitOffset) selectOrdered;

		if (query.getLimit() > 0) {
			limitable = limitable.limit(query.getLimit());
		}

		if (query.getOffset() > 0) {
			limitable = limitable.offset(query.getOffset());
		}
		return (SelectBuilder.SelectOrdered) limitable;
	}

	SelectBuilder.SelectOrdered applyCriteria(@Nullable CriteriaDefinition criteria,
			SelectBuilder.SelectWhere whereBuilder, MapSqlParameterSource parameterSource, Table table) {

		return criteria == null || criteria.isEmpty() // Check for null and empty criteria
				? whereBuilder //
				: whereBuilder.where(queryMapper.getMappedObject(parameterSource, criteria, table, entity));
	}

	/**
	 * Value object representing a {@code JOIN} association.
	 */
	record Join(Table joinTable, List<Pair<Column, Column>> columns) {
	}

	/**
	 * Value object encapsulating column name caches.
	 *
	 * @author Mark Paluch
	 * @author Jens Schauder
	 */
	static class Columns {

		private final MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;
		private final JdbcConverter converter;

		private final List<SqlIdentifier> columnNames = new ArrayList<>();
		private final List<SqlIdentifier> idColumnNames = new ArrayList<>();
		private final List<SqlIdentifier> nonIdColumnNames = new ArrayList<>();
		private final Set<SqlIdentifier> readOnlyColumnNames = new HashSet<>();
		private final Set<SqlIdentifier> insertOnlyColumnNames = new HashSet<>();
		private final Set<SqlIdentifier> insertableColumns;
		private final Set<SqlIdentifier> updatableColumns;

		Columns(RelationalPersistentEntity<?> entity,
				MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext,
				JdbcConverter converter) {

			this.mappingContext = mappingContext;
			this.converter = converter;

			populateColumnNameCache(entity, "");

			Set<SqlIdentifier> insertable = new LinkedHashSet<>(nonIdColumnNames);
			insertable.removeAll(readOnlyColumnNames);

			this.insertableColumns = Collections.unmodifiableSet(insertable);

			Set<SqlIdentifier> updatable = new LinkedHashSet<>(columnNames);

			updatable.removeAll(idColumnNames);
			updatable.removeAll(readOnlyColumnNames);
			updatable.removeAll(insertOnlyColumnNames);

			this.updatableColumns = Collections.unmodifiableSet(updatable);
		}

		private void populateColumnNameCache(RelationalPersistentEntity<?> entity, String prefix) {

			entity.doWithAll(property -> {

				// the referencing column of referenced entity is expected to be on the other side of the relation
				if (!property.isEntity()) {
					initSimpleColumnName(property, prefix);
				} else if (property.isEmbedded()) {
					initEmbeddedColumnNames(property, prefix);
				}
			});
		}

		private void initSimpleColumnName(RelationalPersistentProperty property, String prefix) {

			SqlIdentifier columnName = property.getColumnName().transform(prefix::concat);

			columnNames.add(columnName);

			if (!property.getOwner().isIdProperty(property)) {
				nonIdColumnNames.add(columnName);
			} else {
				idColumnNames.add(columnName);
			}

			if (!property.isWritable()) {
				readOnlyColumnNames.add(columnName);
			}
			if (property.isInsertOnly()) {
				insertOnlyColumnNames.add(columnName);
			}
		}

		private void initEmbeddedColumnNames(RelationalPersistentProperty property, String prefix) {

			String embeddedPrefix = property.getEmbeddedPrefix();

			RelationalPersistentEntity<?> embeddedEntity = mappingContext
					.getRequiredPersistentEntity(converter.getColumnType(property));

			populateColumnNameCache(embeddedEntity, prefix + embeddedPrefix);
		}

		/**
		 * @return Column names that can be used for {@code INSERT}.
		 */
		Set<SqlIdentifier> getInsertableColumns() {
			return insertableColumns;
		}

		/**
		 * @return Column names that can be used for {@code UPDATE}.
		 */
		Set<SqlIdentifier> getUpdatableColumns() {
			return updatableColumns;
		}
	}
}
