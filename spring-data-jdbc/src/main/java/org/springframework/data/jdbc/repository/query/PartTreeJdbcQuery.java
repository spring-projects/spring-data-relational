/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.springframework.data.jdbc.repository.query.JdbcQueryExecution.*;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Lazy;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An {@link AbstractJdbcQuery} implementation based on a {@link PartTree}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Diego Krupitza
 * @author Mikhail Polivakha
 * @since 2.0
 */
public class PartTreeJdbcQuery extends AbstractJdbcQuery {

	private final RelationalMappingContext context;
	private final Parameters<?, ?> parameters;
	private final Dialect dialect;
	private final JdbcConverter converter;
	private final CachedRowMapperFactory cachedRowMapperFactory;
	private final PartTree tree;

	/**
	 * Creates a new {@link PartTreeJdbcQuery}.
	 *
	 * @param context must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapper must not be {@literal null}.
	 */
	public PartTreeJdbcQuery(RelationalMappingContext context, JdbcQueryMethod queryMethod, Dialect dialect,
			JdbcConverter converter, NamedParameterJdbcOperations operations, RowMapper<Object> rowMapper) {
		this(context, queryMethod, dialect, converter, operations, it -> rowMapper);
	}

	/**
	 * Creates a new {@link PartTreeJdbcQuery}.
	 *
	 * @param context must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapperFactory must not be {@literal null}.
	 * @since 2.3
	 */
	public PartTreeJdbcQuery(RelationalMappingContext context, JdbcQueryMethod queryMethod, Dialect dialect,
			JdbcConverter converter, NamedParameterJdbcOperations operations, RowMapperFactory rowMapperFactory) {

		super(queryMethod, operations);

		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(queryMethod, "JdbcQueryMethod must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(rowMapperFactory, "RowMapperFactory must not be null");

		this.context = context;
		this.parameters = queryMethod.getParameters();
		this.dialect = dialect;
		this.converter = converter;

		this.tree = new PartTree(queryMethod.getName(), queryMethod.getResultProcessor().getReturnedType().getDomainType());
		JdbcQueryCreator.validate(this.tree, this.parameters, this.converter.getMappingContext());

		this.cachedRowMapperFactory = new CachedRowMapperFactory(tree, rowMapperFactory, converter,
				queryMethod.getResultProcessor());
	}

	private Sort getDynamicSort(RelationalParameterAccessor accessor) {
		return parameters.potentiallySortsDynamically() ? accessor.getSort() : Sort.unsorted();
	}

	@Override
	public Object execute(Object[] values) {

		RelationalParametersParameterAccessor accessor = new RelationalParametersParameterAccessor(getQueryMethod(),
				values);

		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);
		ParametrizedQuery query = createQuery(accessor, processor.getReturnedType());
		JdbcQueryExecution<?> execution = getQueryExecution(processor, accessor);

		return execution.execute(query.getQuery(), query.getParameterSource(dialect.getLikeEscaper()));
	}

	private JdbcQueryExecution<?> getQueryExecution(ResultProcessor processor,
			RelationalParametersParameterAccessor accessor) {

		ResultSetExtractor<Boolean> extractor = tree.isExistsProjection() ? (ResultSet::next) : null;
		Supplier<RowMapper<?>> rowMapper = parameters.hasDynamicProjection()
				? () -> cachedRowMapperFactory.getRowMapper(processor)
				: cachedRowMapperFactory;

		JdbcQueryExecution<?> queryExecution = getJdbcQueryExecution(extractor, rowMapper);

		if (getQueryMethod().isSliceQuery()) {
			return new SliceQueryExecution<>((JdbcQueryExecution<Collection<Object>>) queryExecution, accessor.getPageable());
		}

		if (getQueryMethod().isPageQuery()) {

			return new PageQueryExecution<>((JdbcQueryExecution<Collection<Object>>) queryExecution, accessor.getPageable(),
					() -> {

						RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();

						JdbcCountQueryCreator queryCreator = new JdbcCountQueryCreator(context, tree, converter, dialect,
								entityMetadata, accessor, false, processor.getReturnedType(), getQueryMethod().lookupLockAnnotation());

						ParametrizedQuery countQuery = queryCreator.createQuery(Sort.unsorted());
						Object count = singleObjectQuery((rs, i) -> rs.getLong(1)).execute(countQuery.getQuery(),
								countQuery.getParameterSource(dialect.getLikeEscaper()));

						return converter.getConversionService().convert(count, Long.class);
					});
		}

		return queryExecution;
	}

	ParametrizedQuery createQuery(RelationalParametersParameterAccessor accessor, ReturnedType returnedType) {

		RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();

		JdbcQueryCreator queryCreator = new JdbcQueryCreator(context, tree, converter, dialect, entityMetadata, accessor,
				getQueryMethod().isSliceQuery(), returnedType, this.getQueryMethod().lookupLockAnnotation());
		return queryCreator.createQuery(getDynamicSort(accessor));
	}

	private JdbcQueryExecution<?> getJdbcQueryExecution(@Nullable ResultSetExtractor<Boolean> extractor,
			Supplier<RowMapper<?>> rowMapper) {

		if (getQueryMethod().isPageQuery() || getQueryMethod().isSliceQuery()) {
			return collectionQuery(rowMapper.get());
		} else {

			if (getQueryMethod().isModifyingQuery()) {
				return createModifyingQueryExecutor();
			} else {
				return createReadingQueryExecution(extractor, rowMapper);
			}
		}
	}

	/**
	 * {@link JdbcQueryExecution} returning a {@link org.springframework.data.domain.Slice}.
	 *
	 * @param <T>
	 */
	static class SliceQueryExecution<T> implements JdbcQueryExecution<Slice<T>> {

		private final JdbcQueryExecution<? extends Collection<T>> delegate;
		private final Pageable pageable;

		public SliceQueryExecution(JdbcQueryExecution<? extends Collection<T>> delegate, Pageable pageable) {
			this.delegate = delegate;
			this.pageable = pageable;
		}

		@Override
		public Slice<T> execute(String query, SqlParameterSource parameter) {

			Collection<T> result = delegate.execute(query, parameter);

			int pageSize = 0;
			if (pageable.isPaged()) {

				pageSize = pageable.getPageSize();
			}

			List<T> resultList = result instanceof List ? (List<T>) result : new ArrayList<>(result);

			boolean hasNext = pageable.isPaged() && resultList.size() > pageSize;

			return new SliceImpl<>(hasNext ? resultList.subList(0, pageSize) : resultList, pageable, hasNext);
		}
	}

	/**
	 * {@link JdbcQueryExecution} returning a {@link org.springframework.data.domain.Page}.
	 *
	 * @param <T>
	 */
	static class PageQueryExecution<T> implements JdbcQueryExecution<Slice<T>> {

		private final JdbcQueryExecution<? extends Collection<T>> delegate;
		private final Pageable pageable;
		private final LongSupplier countSupplier;

		PageQueryExecution(JdbcQueryExecution<? extends Collection<T>> delegate, Pageable pageable,
				LongSupplier countSupplier) {
			this.delegate = delegate;
			this.pageable = pageable;
			this.countSupplier = countSupplier;
		}

		@Override
		public Slice<T> execute(String query, SqlParameterSource parameter) {

			Collection<T> result = delegate.execute(query, parameter);

			return PageableExecutionUtils.getPage(result instanceof List ? (List<T>) result : new ArrayList<>(result),
					pageable, countSupplier);
		}

	}

	/**
	 * Cached implementation of {@link RowMapper} suppler providing either a cached variant of the RowMapper or creating a
	 * new one when using dynamic projections.
	 */
	class CachedRowMapperFactory implements Supplier<RowMapper<?>> {

		private final Lazy<RowMapper<?>> rowMapper;
		private final Function<ResultProcessor, RowMapper<?>> rowMapperFunction;

		public CachedRowMapperFactory(PartTree tree, RowMapperFactory rowMapperFactory, RelationalConverter converter,
				ResultProcessor defaultResultProcessor) {

			this.rowMapperFunction = processor -> {

				if (tree.isCountProjection() || tree.isExistsProjection()) {
					return rowMapperFactory.create(resolveTypeToRead(processor));
				}
				Converter<Object, Object> resultProcessingConverter = new ResultProcessingConverter(processor,
						converter.getMappingContext(), converter.getEntityInstantiators());
				return new ConvertingRowMapper<>(rowMapperFactory.create(processor.getReturnedType().getDomainType()),
						resultProcessingConverter);
			};

			this.rowMapper = Lazy.of(() -> this.rowMapperFunction.apply(defaultResultProcessor));
		}

		@Override
		public RowMapper<?> get() {
			return getRowMapper();
		}

		public RowMapper<?> getRowMapper() {
			return rowMapper.get();
		}

		public RowMapper<?> getRowMapper(ResultProcessor resultProcessor) {
			return rowMapperFunction.apply(resultProcessor);
		}

	}
}
