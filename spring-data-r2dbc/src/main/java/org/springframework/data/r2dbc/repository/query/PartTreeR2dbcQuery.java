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
package org.springframework.data.r2dbc.repository.query;

import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;

/**
 * An {@link AbstractR2dbcQuery} implementation based on a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Diego Krupitza
 * @since 1.1
 */
public class PartTreeR2dbcQuery extends AbstractR2dbcQuery {

	private final ResultProcessor processor;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final RelationalParameters parameters;
	private final PartTree tree;

	/**
	 * Creates new instance of this class with the given {@link R2dbcQueryMethod}, {@link DatabaseClient},
	 * {@link R2dbcConverter} and {@link ReactiveDataAccessStrategy}.
	 *
	 * @param method query method, must not be {@literal null}.
	 * @param entityOperations entity operations, must not be {@literal null}.
	 * @param converter converter, must not be {@literal null}.
	 * @param dataAccessStrategy data access strategy, must not be {@literal null}.
	 */
	public PartTreeR2dbcQuery(R2dbcQueryMethod method, R2dbcEntityOperations entityOperations, R2dbcConverter converter,
			ReactiveDataAccessStrategy dataAccessStrategy) {
		super(method, entityOperations, converter);

		this.processor = method.getResultProcessor();
		this.dataAccessStrategy = dataAccessStrategy;
		this.parameters = method.getParameters();

		try {
			this.tree = new PartTree(method.getName(), processor.getReturnedType()
					.getDomainType());
			R2dbcQueryCreator.validate(this.tree, this.parameters);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s; %s", method, e.getMessage()), e);
		}
	}

	@Override
	protected boolean isModifyingQuery() {
		return this.tree.isDelete();
	}

	@Override
	protected boolean isCountQuery() {
		return this.tree.isCountProjection();
	}

	@Override
	protected boolean isExistsQuery() {
		return this.tree.isExistsProjection();
	}

	@Override
	protected Mono<PreparedOperation<?>> createQuery(RelationalParameterAccessor accessor) {

		return Mono.fromSupplier(() -> {

			ReturnedType returnedType = processor.withDynamicProjection(accessor).getReturnedType();
			List<String> projectedProperties = Collections.emptyList();

			if (returnedType.needsCustomConstruction()) {
				projectedProperties = new ArrayList<>(returnedType.getInputProperties());
			}

			RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();
			R2dbcQueryCreator queryCreator = new R2dbcQueryCreator(tree, dataAccessStrategy, entityMetadata, accessor,
					projectedProperties, this.getQueryMethod().getLock());
			return queryCreator.createQuery(getDynamicSort(accessor));
		});
	}

	private Sort getDynamicSort(RelationalParameterAccessor accessor) {
		return parameters.potentiallySortsDynamically() ? accessor.getSort() : Sort.unsorted();
	}

	@Override
	public String toString() {
		String sb = getClass().getSimpleName()
					+ " [" + getQueryMethod().getName()
					+ ']';
		return sb;
	}
}
