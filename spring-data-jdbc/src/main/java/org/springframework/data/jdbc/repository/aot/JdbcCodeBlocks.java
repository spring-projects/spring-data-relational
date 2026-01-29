/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.javapoet.LordOfTheStrings;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.ParameterBinding;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.query.StatementFactory;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.MethodReturn;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Pair;
import org.springframework.data.util.Streamable;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Common code blocks for JDBC AOT Fragment generation.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.0
 */
class JdbcCodeBlocks {

	/**
	 * @return new {@link QueryBlockBuilder}.
	 */
	public static QueryBlockBuilder queryBuilder(AotQueryMethodGenerationContext context, JdbcQueryMethod queryMethod) {
		return new QueryBlockBuilder(context, queryMethod);
	}

	/**
	 * @return new {@link QueryExecutionBlockBuilder}.
	 */
	static QueryExecutionBlockBuilder executionBuilder(AotQueryMethodGenerationContext context,
			JdbcQueryMethod queryMethod) {
		return new QueryExecutionBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for the actual query code block.
	 */
	static class QueryBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final JdbcQueryMethod queryMethod;
		private final String parameterNames;
		private String queryVariableName = "undefined";
		private String parameterSourceVariableName = "undefined";
		private @Nullable AotQueries queries;
		private MergedAnnotation<Lock> lock = MergedAnnotation.missing();

		private QueryBlockBuilder(AotQueryMethodGenerationContext context, JdbcQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;

			String parameterNames = StringUtils.collectionToDelimitedString(context.getAllParameterNames(), ", ");

			if (StringUtils.hasText(parameterNames)) {
				this.parameterNames = ", " + parameterNames;
			} else {
				this.parameterNames = "";
			}
		}

		public QueryBlockBuilder usingQueryVariableName(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		public QueryBlockBuilder parameterSource(String parameterSource) {

			this.parameterSourceVariableName = parameterSource;
			return this;
		}

		public QueryBlockBuilder filter(AotQueries query) {
			this.queries = query;
			return this;
		}

		public QueryBlockBuilder lock(MergedAnnotation<Lock> lock) {
			this.lock = lock;
			return this;
		}

		/**
		 * Build the query block.
		 *
		 * @return
		 */
		public CodeBlock build() {

			Assert.notNull(queries, "Queries must not be null");

			if (queries.result() instanceof DerivedAotQuery derivedQuery) {
				return createDerivedQuery(derivedQuery,
						queries.count() instanceof DerivedAotQuery derivedCountQuery ? derivedCountQuery : null);
			}

			if (queries.result() instanceof StringAotQuery stringQuery) {
				return createStringQuery(queryVariableName, parameterSourceVariableName, stringQuery);
			}

			throw new IllegalArgumentException("Unsupported AOT query type: " + queries.result());
		}

		private CodeBlock createDerivedQuery(DerivedAotQuery entityQuery, @Nullable DerivedAotQuery countQuery) {

			CriteriaDefinition criteria = entityQuery.getCriteria();

			Builder builder = CodeBlock.builder();

			if (!criteria.isEmpty()) {
				builder.add(buildCriteria(criteria, (criteriaDefinition, b) -> {

					Assert.state(criteriaDefinition.getColumn() != null, "Criteria must have a column");

					b.add("$[$1T $2L = $1T.where($3S)", Criteria.class, context.localVariable("criteria"),
							criteriaDefinition.getColumn().getReference());
				}, b -> b.add(";\n$]")));
			}

			builder.add(buildQuery(false, entityQuery, criteria));

			if (countQuery != null) {

				Builder countAll = CodeBlock.builder();

				countAll.beginControlFlow("$T $L = () ->", LongSupplier.class, context.localVariable("countAll"));

				countAll.add(buildQuery(true, countQuery, criteria));

				countAll.addStatement("$1T $2L = $3L.executeWith(($4L, $5L) -> queryForObject($4L, $5L, new $6T<>($1T.class)))",
						Number.class, context.localVariable("count"), context.localVariable("countBuilder"),
						context.localVariable("sql"), context.localVariable("paramSource"), SingleColumnRowMapper.class);

				countAll.addStatement("return $1L != null ? $1L.longValue() : 0L", context.localVariable("count"));

				// end control flow does not work well with lambdas
				countAll.unindent();
				countAll.add("};\n");

				builder.add("\n");
				builder.add(countAll.build());
			}

			return builder.build();
		}

		private CodeBlock buildQuery(boolean count, DerivedAotQuery aotQuery, CriteriaDefinition criteria) {

			Builder builder = CodeBlock.builder();
			String selection = context.localVariable(count ? "countBuilder" : "builder");

			String method;
			if (aotQuery.isCount()) {
				method = "count($T.class)";
			} else if (aotQuery.isExists()) {
				method = "exists($T.class)";
			} else if (queryMethod.isSliceQuery()) {
				method = "slice($T.class)";
			} else {
				method = "select($T.class)";
			}

			builder.add("$[");
			builder.add("$T $L = getStatementFactory()." + method, StatementFactory.SelectionBuilder.class, selection,
					context.getRepositoryInformation().getDomainType());

			if (!aotQuery.isCount() && !aotQuery.isExists()) {

				if (aotQuery.isLimited()) {
					builder.add(".limit($L)", aotQuery.getLimit().max());
				} else if (StringUtils.hasText(context.getLimitParameterName())) {
					builder.add(".limit($L)", context.getLimitParameterName());
				}

				if (StringUtils.hasText(context.getPageableParameterName())) {
					builder.add(".page($L)", context.getPageableParameterName());
				}

				Sort sort = aotQuery.getSort();
				if (sort.isSorted()) {
					builder.add(".orderBy($L)", buildSort(sort));
				}

				if (StringUtils.hasText(context.getSortParameterName())) {
					builder.add(".orderBy($L)", context.getSortParameterName());
				}
			}

			if (lock.isPresent()) {
				builder.add(".lock($T.$L)", LockMode.class, lock.getEnum("value", LockMode.class).name());
			}

			if (!criteria.isEmpty()) {
				builder.add(".filter($L)", context.localVariable("criteria"));
			}

			builder.add(";\n$]");

			return builder.build();
		}

		private static CodeBlock buildSort(Sort sort) {

			Builder sortBuilder = CodeBlock.builder();
			sortBuilder.add("$T.by(", Sort.class);

			boolean first = true;
			for (Sort.Order order : sort) {

				sortBuilder.add("$T.$L($S)", Sort.Order.class, order.isAscending() ? "asc" : "desc", order.getProperty());
				if (order.isIgnoreCase()) {
					sortBuilder.add(".ignoreCase()");
				}

				if (first) {
					first = false;
				} else {
					sortBuilder.add(", ");
				}
			}

			sortBuilder.add(")");

			return sortBuilder.build();
		}

		private CodeBlock buildCriteria(CriteriaDefinition criteria, BiConsumer<CriteriaDefinition, Builder> preamble,
				Consumer<CodeBlock.Builder> end) {

			CriteriaDefinition current = criteria;

			// reverse unroll criteria chain
			Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

			Builder builder = CodeBlock.builder();

			while (current.hasPrevious()) {
				forwardChain.put(current.getRequiredPrevious(), current);
				current = current.getRequiredPrevious();
			}

			preamble.accept(current, builder);
			appendCriteria(current, builder);

			while ((current = forwardChain.get(current)) != null) {

				if (current.isEmpty()) {
					continue;
				}

				if (current.isGroup()) {

					String suffix;

					builder.add(".$L(", current.getCombinator().name().toLowerCase(Locale.ROOT));

					if (current.getGroup().size() == 1) {
						suffix = ")";

					} else {
						suffix = "))";
						builder.add("$T.of(", List.class);
					}

					for (CriteriaDefinition nested : current.getGroup()) {

						builder.add(buildCriteria(nested, (criteriaDefinition, start) -> {

							Assert.state(criteriaDefinition.getColumn() != null, "Criteria must have a column");

							start.add("$T.where($S)", Criteria.class, criteriaDefinition.getColumn().getReference());
						}, b -> {}));
					}

					builder.add(suffix);
					continue;
				}

				Assert.state(current.getColumn() != null, "Criteria must have a column");
				builder.add(".$L($S)", builder.add(".$L(", current.getCombinator().name().toLowerCase(Locale.ROOT)),
						current.getColumn().getReference());

				appendCriteria(current, builder);
			}

			end.accept(builder);

			return builder.build();
		}

		private void appendCriteria(CriteriaDefinition current, Builder builder) {

			Object value = current.getValue();
			Assert.notNull(current.getComparator(), "Comparator must not be null");

			switch (current.getComparator()) {
				case INITIAL -> {}
				case EQ -> builder.add(".is($L)", renderPlaceholder(value));
				case NEQ -> builder.add(".not($L)", renderPlaceholder(value));
				case BETWEEN -> builder.add(".between($L, $L)", renderPlaceholder(value, 0), renderPlaceholder(value, 1));
				case NOT_BETWEEN ->
					builder.add(".notBetween($L, $L)", renderPlaceholder(value, 0), renderPlaceholder(value, 1));
				case LT -> builder.add(".lessThan($L)", renderPlaceholder(value));
				case LTE -> builder.add(".lessThanOrEquals($L)", renderPlaceholder(value));
				case GT -> builder.add(".greaterThan($L)", renderPlaceholder(value));
				case GTE -> builder.add(".greaterThanEquals($L)", renderPlaceholder(value));
				case IS_NULL -> builder.add(".isNull()");
				case IS_NOT_NULL -> builder.add(".isNotNull()");
				case LIKE -> applyLike(builder, "like", value);
				case NOT_LIKE -> applyLike(builder, "notLike", value);
				case NOT_IN -> builder.add(".notIn($L)", renderPlaceholder(value));
				case IN -> builder.add(".in($L)", renderPlaceholder(value));
				case IS_TRUE -> builder.add(".isTrue()");
				case IS_FALSE -> builder.add(".isFalse()");
			}

			if (current.isIgnoreCase()) {
				builder.add(".ignoreCase(true)");
			}
		}

		private void applyLike(Builder builder, String method, @Nullable Object value) {

			PlaceholderAccessor.CapturingJdbcValue captured = PlaceholderAccessor.unwrap(value);

			String likeValue = "$L";
			if (captured.getBinding() instanceof ParameterBinding.LikeParameterBinding lpb) {

				if (lpb.getType() == Part.Type.CONTAINING) {
					likeValue = "\"%\" + escape($L) + \"%\"";
				} else if (lpb.getType() == Part.Type.STARTING_WITH) {
					likeValue = "escape($L) + \"%\"";
				} else if (lpb.getType() == Part.Type.ENDING_WITH) {
					likeValue = "\"%\" + escape($L)";
				}
			}

			builder.add(".$L(" + likeValue + ")", method, renderPlaceholder(value));
		}

		private String renderPlaceholder(@Nullable Object value) {

			PlaceholderAccessor.CapturingJdbcValue captured = PlaceholderAccessor.unwrap(value);

			ParameterBinding binding = captured.getBinding();
			ParameterBinding.MethodInvocationArgument argument = (ParameterBinding.MethodInvocationArgument) binding
					.getOrigin();
			ParameterBinding.BindingIdentifier identifier = argument.identifier();

			return identifier.hasName() ? identifier.getName()
					: Objects.requireNonNull(context.getParameterName(identifier.getPosition()));
		}

		private Object renderPlaceholder(@Nullable Object value, int index) {

			Assert.isInstanceOf(Pair.class, value, "Value must be of type Pair");

			return renderPlaceholder(index == 0 ? ((Pair) value).getFirst() : ((Pair) value).getSecond());
		}

		private CodeBlock createStringQuery(String queryVariableName, String parameterSourceName, StringAotQuery query) {

			Builder builder = CodeBlock.builder();

			builder.addStatement("$T $L = $S", String.class, queryVariableName, query.getQueryString());
			builder.addStatement("$1T $2L = new $1T()", MapSqlParameterSource.class, parameterSourceName);

			for (ParameterBinding binding : query.getParameterBindings()) {

				String parameterIdentifier = getParameterName(binding.getIdentifier());
				builder.add(bindValue(parameterIdentifier, binding.getOrigin()));
			}

			return builder.build();
		}

		private String getParameterName(ParameterBinding.BindingIdentifier identifier) {
			return identifier.hasName() ? identifier.getName() : Integer.toString(identifier.getPosition());
		}

		private CodeBlock bindValue(String parameterName, ParameterBinding.ParameterOrigin origin) {

			if (origin.isMethodArgument() && origin instanceof ParameterBinding.MethodInvocationArgument mia) {

				String parameterReference;
				String identifier;

				if (mia.identifier().hasPosition()) {
					parameterReference = context.getRequiredBindableParameterName(mia.identifier().getPosition() - 1);
					identifier = Integer.toString(mia.identifier().getPosition() - 1);
				} else {
					parameterReference = context.getRequiredBindableParameterName(mia.identifier().getName());
					identifier = mia.identifier().getName();
				}

				Builder builder = CodeBlock.builder();
				builder.addStatement("getBindableValue($L, $L, $L).bind($S, $L)",
						context.getExpressionMarker().enclosingMethod(), parameterReference, identifier, parameterName,
						parameterSourceVariableName);
				return builder.build();
			}

			if (origin.isExpression() && origin instanceof ParameterBinding.Expression expr) {

				Builder builder = CodeBlock.builder();

				String expressionString = expr.expression().getExpressionString();
				// re-wrap expression
				if (!expressionString.startsWith("$")) {
					expressionString = "#{" + expressionString + "}";
				}

				builder.addStatement("evaluate($L, $S$L).bind($S, $L)", context.getExpressionMarker().enclosingMethod(),
						expressionString, parameterNames, parameterName, parameterSourceVariableName);
				return builder.build();
			}

			throw new UnsupportedOperationException("Not supported yet for: " + origin);
		}

	}

	static class QueryExecutionBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final JdbcQueryMethod queryMethod;
		private @Nullable AotQuery aotQuery;
		private String queryVariableName = "undefined";
		private String parameterSourceVariableName = "undefined";
		private @Nullable String rowMapperRef;
		private @Nullable Class<?> rowMapperClass;
		private @Nullable String resultSetExtractorRef;
		private @Nullable Class<?> resultSetExtractorClass;
		private MergedAnnotation<Modifying> modifying = MergedAnnotation.missing();

		private QueryExecutionBlockBuilder(AotQueryMethodGenerationContext context, JdbcQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		public QueryExecutionBlockBuilder queryAnnotation(MergedAnnotation<Query> query) {

			if (query.isPresent()) {
				rowMapper(query.getClass("rowMapperClass"));
				rowMapper(query.getString("rowMapperRef"));
				resultSetExtractor(query.getClass("resultSetExtractorClass"));
				resultSetExtractor(query.getString("resultSetExtractorRef"));
			}

			return this;
		}

		public QueryExecutionBlockBuilder rowMapper(String rowMapperRef) {
			this.rowMapperRef = rowMapperRef;
			return this;
		}

		public QueryExecutionBlockBuilder rowMapper(Class<?> rowMapperClass) {
			this.rowMapperClass = rowMapperClass == RowMapper.class ? null : rowMapperClass;
			return this;
		}

		public QueryExecutionBlockBuilder resultSetExtractor(String resultSetExtractorRef) {
			this.resultSetExtractorRef = resultSetExtractorRef;
			return this;
		}

		public QueryExecutionBlockBuilder resultSetExtractor(Class<?> resultSetExtractorClass) {
			this.resultSetExtractorClass = resultSetExtractorClass == ResultSetExtractor.class ? null
					: resultSetExtractorClass;
			return this;
		}

		public QueryExecutionBlockBuilder usingQueryVariableName(String queryVariableName) {

			this.queryVariableName = queryVariableName;
			return this;
		}

		public QueryExecutionBlockBuilder parameterSource(String parameterSourceVariableName) {

			this.parameterSourceVariableName = parameterSourceVariableName;
			return this;
		}

		public QueryExecutionBlockBuilder queries(AotQueries aotQueries) {

			this.aotQuery = aotQueries.result();
			return this;
		}

		public QueryExecutionBlockBuilder modifying(MergedAnnotation<Modifying> modifying) {

			this.modifying = modifying;
			return this;
		}

		public CodeBlock build() {

			Assert.state(aotQuery != null, "AOT Query must not be null");

			return doBuild();
		}

		@SuppressWarnings("NullAway")
		private CodeBlock doBuild() {

			MethodReturn methodReturn = context.getMethodReturn();
			boolean isProjecting = methodReturn.isProjecting()
					|| StringUtils.hasText(context.getDynamicProjectionParameterName());
			Type actualReturnType = isProjecting ? methodReturn.getActualReturnClass()
					: context.getRepositoryInformation().getDomainType();

			Class<?> returnType = context.getMethodReturn().toClass();

			TypeName queryResultType = methodReturn.getActualClassName();
			String result = context.localVariable("result");
			String rowMapper = context.localVariable("rowMapper");

			ExecutionDecorator decorator = getExecutionDecorator();

			if (modifying.isPresent()) {
				return update(decorator, returnType);
			} else if (aotQuery.isCount()) {
				return count(decorator, result, queryResultType, returnType);
			} else if (aotQuery.isExists()) {
				return exists(decorator, queryResultType);
			} else if (aotQuery.isDelete()) {
				return delete(decorator, rowMapper, result, queryResultType, returnType, actualReturnType);
			} else {
				return select(decorator, rowMapper, result, queryResultType, isProjecting, methodReturn);
			}
		}

		private ExecutionDecorator getExecutionDecorator() {

			if (aotQuery instanceof DerivedAotQuery) {

				return new ExecutionDecorator() {
					@Override
					public String decorate(String executionCode) {
						String builder = context.localVariable("builder");

						return String.format("%s.executeWith((%s, %s) -> %s)", builder, query(), paramSource(), executionCode);
					}

					@Override
					public String query() {
						return context.localVariable("sql");
					}

					@Override
					public String paramSource() {
						return context.localVariable("paramSource");
					}
				};
			}
			return new ExecutionDecorator() {
				@Override
				public String decorate(String executionCode) {
					return executionCode;
				}

				@Override
				public String query() {
					return queryVariableName;
				}

				@Override
				public String paramSource() {
					return parameterSourceVariableName;
				}
			};

		}

		/**
		 * Decorates an execution. Used to wrap execution with lambda or similar.
		 */
		interface ExecutionDecorator {

			/**
			 * Decorate the given executionCode with a wrapper.
			 *
			 * @param executionCode
			 * @return
			 */
			String decorate(String executionCode);

			String query();

			String paramSource();

		}

		private CodeBlock update(ExecutionDecorator decorator, Class<?> returnType) {

			String result = context.localVariable("result");

			Builder builder = CodeBlock.builder();

			LordOfTheStrings.InvocationBuilder invoke = LordOfTheStrings
					.invoke(decorator.decorate("getJdbcOperations().update($L, $L)"), decorator.query(), decorator.paramSource());

			if (context.getMethodReturn().isVoid()) {
				builder.addStatement(invoke.build());
			} else {
				builder.addStatement(invoke.assignTo("int $L", result));
			}

			builder.addStatement(LordOfTheStrings.returning(returnType) //
					.whenBoolean("$L != 0", result) //
					.whenBoxedLong("(long) $L", result) //
					.otherwise("$L", result)//
					.build());

			return builder.build();
		}

		private CodeBlock delete(ExecutionDecorator decorator, String rowMapper, String result, TypeName queryResultType,
				Class<?> returnType, Type actualReturnType) {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.addStatement("$T $L = getRowMapperFactory().create($T.class)", RowMapper.class, rowMapper,
					context.getRepositoryInformation().getDomainType());

			builder.addStatement(
					"$1T $2L = ($1T) " + decorator.decorate("getJdbcOperations().query($3L, $4L, new $5T<>($6L))"), List.class,
					result, decorator.query(), decorator.paramSource(), RowMapperResultSetExtractor.class, rowMapper);

			builder.addStatement("$L.forEach(getOperations()::delete)", result);

			builder.addStatement(LordOfTheStrings.returning(returnType) //
					.when(Collection.class.isAssignableFrom(context.getMethodReturn().toClass()),
							"($T) convertMany($L, $T.class)", context.getMethodReturn().getTypeName(), result, queryResultType) //
					.when(context.getRepositoryInformation().getDomainType(),
							"($1T) ($2L.isEmpty() ? null : $2L.iterator().next())", actualReturnType, result) //
					.whenBoolean("!$L.isEmpty()", result) //
					.whenBoxedLong("(long) $L.size()", result) //
					.otherwise("$L.size()", result) //
					.build());

			return builder.build();
		}

		private CodeBlock count(ExecutionDecorator decorator, String result, TypeName queryResultType,
				Class<?> returnType) {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.addStatement("$1T $2L = " + decorator.decorate("queryForObject($3L, $4L, new $5T<>($1T.class))"),
					Number.class, result, decorator.query(), decorator.paramSource(), SingleColumnRowMapper.class);

			builder.addStatement(LordOfTheStrings.returning(returnType) //
					.number(result) //
					.whenPrimitiveOrBoxed(short.class, "$1L.shortValue()", result) //
					.whenPrimitiveOrBoxed(byte.class, "$1L.byteValue()", result) //
					.whenPrimitiveOrBoxed(double.class, "$1L.doubleValue()", result) //
					.whenPrimitiveOrBoxed(float.class, "$1L.floatValue()", result) //
					.otherwise("($T) convertOne($L, $T.class)", context.getMethodReturn().getTypeName(), result, queryResultType) //
					.build());

			return builder.build();
		}

		private CodeBlock exists(ExecutionDecorator decorator, TypeName queryResultType) {

			CodeBlock.Builder builder = CodeBlock.builder();

			builder.addStatement("return ($T) " + decorator.decorate("getJdbcOperations().query($L, $L, $T::next)"),
					queryResultType, decorator.query(), decorator.paramSource(), ResultSet.class);

			return builder.build();
		}

		private CodeBlock select(ExecutionDecorator decorator, String rowMapper, String result, TypeName queryResultType,
				boolean isProjecting, MethodReturn methodReturn) {

			Builder builder = CodeBlock.builder();

			String resultSetExtractor = null;

			if (rowMapperClass != null) {
				builder.addStatement("$T $L = new $T()", RowMapper.class, rowMapper, rowMapperClass);
			} else if (StringUtils.hasText(rowMapperRef)) {
				builder.addStatement("$T $L = getRowMapperFactory().getRowMapper($S)", RowMapper.class, rowMapper,
						rowMapperRef);
			} else if (resultSetExtractorClass == null) {

				Type typeToRead;

				if (isProjecting) {
					typeToRead = context.getReturnedType().getDomainType();
				} else {
					typeToRead = methodReturn.getActualReturnClass();
				}

				builder.addStatement("$T $L = getRowMapperFactory().create($T.class)", RowMapper.class, rowMapper, typeToRead);
			}

			if (StringUtils.hasText(resultSetExtractorRef) || resultSetExtractorClass != null) {

				resultSetExtractor = context.localVariable("resultSetExtractor");

				if (resultSetExtractorClass != null && (rowMapperClass != null || StringUtils.hasText(rowMapperRef))) {
					builder.addStatement("$T $L = new $T($L)", ResultSetExtractor.class, resultSetExtractor,
							resultSetExtractorClass, rowMapper);
				} else if (resultSetExtractorClass != null) {
					builder.addStatement("$T $L = new $T()", ResultSetExtractor.class, resultSetExtractor,
							resultSetExtractorClass);
				} else if (StringUtils.hasText(resultSetExtractorRef)) {
					builder.addStatement("$T $L = getRowMapperFactory().getResultSetExtractor($S)", ResultSetExtractor.class,
							resultSetExtractor, resultSetExtractorRef);
				}
			}

			if (StringUtils.hasText(resultSetExtractor)) {

				builder.addStatement("return ($T) " + decorator.decorate("getJdbcOperations().query($L, $L, $L)"),
						queryResultType, decorator.query(), decorator.paramSource(), resultSetExtractor);

				return builder.build();
			}

			boolean dynamicProjection = StringUtils.hasText(context.getDynamicProjectionParameterName());
			Object queryResultTypeRef = dynamicProjection ? context.getDynamicProjectionParameterName() : queryResultType;

			if (queryMethod.isCollectionQuery() || queryMethod.isSliceQuery() || queryMethod.isPageQuery()) {

				builder.addStatement(
						"$1T $2L = ($1T) " + decorator.decorate("getJdbcOperations().query($3L, $4L, new $5T<>($6L))"), List.class,
						result, decorator.query(), decorator.paramSource(), RowMapperResultSetExtractor.class, rowMapper);

				if (queryMethod.isSliceQuery() || queryMethod.isPageQuery()) {

					String pageable = context.getPageableParameterName();

					builder.addStatement(
							"$1T $2L = ($1T) convertMany($3L, %s)".formatted(dynamicProjection ? "$4L" : "$4T.class"), List.class,
							context.localVariable("converted"), result, queryResultTypeRef);

					if (queryMethod.isPageQuery()) {

						builder.addStatement("return $1T.getPage($2L, $3L, $4L)", PageableExecutionUtils.class,
								context.localVariable("converted"), pageable, context.localVariable("countAll"));
					} else {

						builder.addStatement("boolean $1L = $2L.isPaged() && $3L.size() > $2L.getPageSize()",
								context.localVariable("hasNext"), pageable, context.localVariable("converted"));

						builder.addStatement("return new $1T($2L ? $3L.subList(0, $4L.getPageSize()) : $3L, $4L, $2L)",
								SliceImpl.class, context.localVariable("hasNext"), context.localVariable("converted"), pageable);
					}

					return builder.build();
				}

				if (isStreamable(methodReturn)) {
					builder.addStatement(
							"return ($1T) $1T.of(($2T) convertMany($3L, %s))".formatted(dynamicProjection ? "$4L" : "$4T.class"),
							Streamable.class, Iterable.class, result, queryResultTypeRef);
				} else if (isStreamableWrapper(methodReturn) && canConvert(Streamable.class, methodReturn)) {

					builder.addStatement(
							"$1T $2L = ($1T) convertMany($3L, %s)".formatted(dynamicProjection ? "$4L" : "$4T.class"), Iterable.class,
							context.localVariable("converted"), result, queryResultTypeRef);

					builder.addStatement(
							"return ($1T) $2T.getSharedInstance().convert($3T.of($4L), $5T.valueOf($3T.class), $5T.valueOf($1T.class))",
							methodReturn.toClass(), DefaultConversionService.class, Streamable.class,
							context.localVariable("converted"), TypeDescriptor.class);
				} else {

					builder.addStatement("return ($T) convertMany($L, %s)".formatted(dynamicProjection ? "$L" : "$T.class"),
							methodReturn.getTypeName(), result, queryResultTypeRef);
				}
			} else if (queryMethod.isStreamQuery()) {

				builder.addStatement("$1T $2L = " + decorator.decorate("getJdbcOperations().queryForStream($3L, $4L, $5L)"),
						Stream.class, result, decorator.query(), decorator.paramSource(), rowMapper);
				builder.addStatement("return ($T) convertMany($L, $T.class)", methodReturn.getTypeName(), result,
						queryResultTypeRef);
			} else {

				builder.addStatement("$T $L = " + decorator.decorate("queryForObject($L, $L, $L)"), Object.class, result,
						decorator.query(), decorator.paramSource(), rowMapper);

				if (methodReturn.isOptional()) {
					builder.addStatement(
							"return ($1T) $1T.ofNullable(convertOne($2L, %s))".formatted(dynamicProjection ? "$3L" : "$3T.class"),
							Optional.class, result, queryResultTypeRef);
				} else {
					builder.addStatement("return ($T) convertOne($L, %s)".formatted(dynamicProjection ? "$L" : "$T.class"),
							methodReturn.getTypeName(), result, queryResultTypeRef);
				}
			}
			return builder.build();
		}

		private boolean canConvert(Class<?> from, MethodReturn methodReturn) {
			return DefaultConversionService.getSharedInstance().canConvert(from, methodReturn.toClass());
		}

		private static boolean isStreamable(MethodReturn methodReturn) {
			return methodReturn.toClass().equals(Streamable.class);
		}

		private static boolean isStreamableWrapper(MethodReturn methodReturn) {
			return !isStreamable(methodReturn) && Streamable.class.isAssignableFrom(methodReturn.toClass());
		}

		public static boolean returnsModifying(Class<?> returnType) {

			return ClassUtils.resolvePrimitiveIfNecessary(returnType) == Integer.class
					|| ClassUtils.resolvePrimitiveIfNecessary(returnType) == Long.class;
		}

	}

}
