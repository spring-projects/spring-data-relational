/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.dialect.IndexedBindMarker;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link StatementFactory} implementation.
 *
 * @author Mark Paluch
 */
@RequiredArgsConstructor
class DefaultStatementFactory implements StatementFactory {

	private final Dialect dialect;
	private final RenderContext renderContext;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementFactory#select(java.lang.String, java.util.Collection, java.util.function.Consumer)
	 */
	@Override
	public PreparedOperation<Select> select(String tableName, Collection<String> columnNames,
			Consumer<StatementBinderBuilder> binderConsumer) {

		Assert.hasText(tableName, "Table must not be empty");
		Assert.notEmpty(columnNames, "Columns must not be empty");
		Assert.notNull(binderConsumer, "Binder Consumer must not be null");

		DefaultBinderBuilder binderBuilder = new DefaultBinderBuilder() {
			@Override
			public void bind(String identifier, SettableValue settable) {
				throw new InvalidDataAccessApiUsageException("Binding for SELECT not supported. Use filterBy(…)");
			}
		};

		binderConsumer.accept(binderBuilder);

		return withDialect((dialect, renderContext) -> {
			Table table = Table.create(tableName);
			List<Column> columns = table.columns(columnNames);
			SelectBuilder.SelectFromAndJoin selectBuilder = StatementBuilder.select(columns).from(table);

			BindMarkers bindMarkers = dialect.getBindMarkersFactory().create();
			Binding binding = binderBuilder.build(table, bindMarkers);
			Select select;

			if (binding.hasCondition()) {
				select = selectBuilder.where(binding.getCondition()).build();
			} else {
				select = selectBuilder.build();
			}

			return new DefaultPreparedOperation<>( //
					select, //
					renderContext, //
					createBindings(binding) //
			);
		});
	}

	@NotNull
	private static Bindings createBindings(Binding binding) {

		List<Bindings.SingleBinding> singleBindings = new ArrayList<>();

		binding.getNulls().forEach( //
				(bindMarker, settableValue) -> {

					if (bindMarker instanceof IndexedBindMarker) {
						singleBindings //
								.add(new Bindings.IndexedSingleBinding( //
										((IndexedBindMarker) bindMarker).getIndex(), //
										settableValue) //
						);
					}
				});

		binding.getValues().forEach( //
				(bindMarker, value) -> {
					if (bindMarker instanceof IndexedBindMarker) {
						singleBindings //
								.add(new Bindings.IndexedSingleBinding( //
										((IndexedBindMarker) bindMarker).getIndex(), //
										value instanceof SettableValue ? (SettableValue) value : SettableValue.from(value)) //
						);
					}
				});

		return new Bindings(singleBindings);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementFactory#insert(java.lang.String, java.util.Collection, java.util.function.Consumer)
	 */
	@Override
	public PreparedOperation<Insert> insert(String tableName, Collection<String> generatedKeysNames,
			Consumer<StatementBinderBuilder> binderConsumer) {

		Assert.hasText(tableName, "Table must not be empty");
		Assert.notNull(generatedKeysNames, "Generated key names must not be null");
		Assert.notNull(binderConsumer, "Binder Consumer must not be null");

		DefaultBinderBuilder binderBuilder = new DefaultBinderBuilder() {
			@Override
			public void filterBy(String identifier, SettableValue settable) {
				throw new InvalidDataAccessApiUsageException("Filter-Binding for INSERT not supported. Use bind(…)");
			}
		};

		binderConsumer.accept(binderBuilder);

		return withDialect((dialect, renderContext) -> {

			BindMarkers bindMarkers = dialect.getBindMarkersFactory().create();
			Table table = Table.create(tableName);

			Map<BindMarker, SettableValue> expressionBindings = new LinkedHashMap<>();
			List<Expression> expressions = new ArrayList<>();
			binderBuilder.forEachBinding((column, settableValue) -> {

				BindMarker bindMarker = bindMarkers.next(column);

				expressions.add(SQL.bindMarker(bindMarker.getPlaceholder()));
				expressionBindings.put(bindMarker, settableValue);
			});

			if (expressions.isEmpty()) {
				throw new IllegalStateException("INSERT contains no value expressions");
			}

			Binding binding = binderBuilder.build(table, bindMarkers).withBindings(expressionBindings);
			Insert insert = StatementBuilder.insert().into(table).columns(table.columns(binderBuilder.bindings.keySet()))
					.values(expressions).build();

			return new DefaultPreparedOperation<Insert>(insert, renderContext, createBindings(binding)) {
				@Override
				public Statement bind(Statement to) {
					return super.bind(to).returnGeneratedValues(generatedKeysNames.toArray(new String[0]));
				}
			};
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementFactory#update(java.lang.String, java.util.function.Consumer)
	 */
	@Override
	public PreparedOperation<Update> update(String tableName, Consumer<StatementBinderBuilder> binderConsumer) {

		Assert.hasText(tableName, "Table must not be empty");
		Assert.notNull(binderConsumer, "Binder Consumer must not be null");

		DefaultBinderBuilder binderBuilder = new DefaultBinderBuilder();

		binderConsumer.accept(binderBuilder);

		return withDialect((dialect, renderContext) -> {

			BindMarkers bindMarkers = dialect.getBindMarkersFactory().create();
			Table table = Table.create(tableName);

			Map<BindMarker, SettableValue> assignmentBindings = new LinkedHashMap<>();
			List<Assignment> assignments = new ArrayList<>();
			binderBuilder.forEachBinding((column, settableValue) -> {

				BindMarker bindMarker = bindMarkers.next(column);
				AssignValue assignment = table.column(column).set(SQL.bindMarker(bindMarker.getPlaceholder()));

				assignments.add(assignment);
				assignmentBindings.put(bindMarker, settableValue);
			});

			if (assignments.isEmpty()) {
				throw new IllegalStateException("UPDATE contains no assignments");
			}

			UpdateBuilder.UpdateWhere updateBuilder = StatementBuilder.update(table).set(assignments);

			Binding binding = binderBuilder.build(table, bindMarkers).withBindings(assignmentBindings);
			Update update;

			if (binding.hasCondition()) {
				update = updateBuilder.where(binding.getCondition()).build();
			} else {
				update = updateBuilder.build();
			}

			return new DefaultPreparedOperation<>(update, renderContext, createBindings(binding));
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementFactory#delete(java.lang.String, java.util.function.Consumer)
	 */
	@Override
	public PreparedOperation<Delete> delete(String tableName, Consumer<StatementBinderBuilder> binderConsumer) {

		Assert.hasText(tableName, "Table must not be empty");
		Assert.notNull(binderConsumer, "Binder Consumer must not be null");

		DefaultBinderBuilder binderBuilder = new DefaultBinderBuilder() {
			@Override
			public void bind(String identifier, SettableValue settable) {
				throw new InvalidDataAccessApiUsageException("Binding for DELETE not supported. Use filterBy(…)");
			}
		};

		binderConsumer.accept(binderBuilder);

		return withDialect((dialect, renderContext) -> {

			Table table = Table.create(tableName);
			DeleteBuilder.DeleteWhere deleteBuilder = StatementBuilder.delete().from(table);

			BindMarkers bindMarkers = dialect.getBindMarkersFactory().create();
			Binding binding = binderBuilder.build(table, bindMarkers);
			Delete delete;

			if (binding.hasCondition()) {
				delete = deleteBuilder.where(binding.getCondition()).build();
			} else {
				delete = deleteBuilder.build();
			}

			return new DefaultPreparedOperation<>(delete, renderContext, createBindings(binding));
		});
	}

	private <T> T withDialect(BiFunction<Dialect, RenderContext, T> action) {

		Assert.notNull(action, "Action must not be null");

		return action.apply(this.dialect, this.renderContext);
	}

	/**
	 * Default {@link StatementBinderBuilder} implementation.
	 */
	static class DefaultBinderBuilder implements StatementBinderBuilder {

		final Map<String, SettableValue> filters = new LinkedHashMap<>();
		final Map<String, SettableValue> bindings = new LinkedHashMap<>();

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementFactory.StatementBinderBuilder#filterBy(java.lang.String, org.springframework.data.r2dbc.domain.SettableValue)
		 */
		@Override
		public void filterBy(String identifier, SettableValue settable) {

			Assert.hasText(identifier, "FilterBy identifier must not be empty");
			Assert.notNull(settable, "SettableValue for Filter must not be null");
			this.filters.put(identifier, settable);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementFactory.StatementBinderBuilder#bind(java.lang.String, org.springframework.data.r2dbc.domain.SettableValue)
		 */
		@Override
		public void bind(String identifier, SettableValue settable) {

			Assert.hasText(identifier, "Bind value identifier must not be empty");
			Assert.notNull(settable, "SettableValue must not be null");

			this.bindings.put(identifier, settable);
		}

		/**
		 * Call {@link BiConsumer} for each filter binding.
		 *
		 * @param consumer the consumer to notify.
		 */
		void forEachFilter(BiConsumer<String, SettableValue> consumer) {
			filters.forEach(consumer);
		}

		/**
		 * Call {@link BiConsumer} for each value binding.
		 *
		 * @param consumer the consumer to notify.
		 */
		void forEachBinding(BiConsumer<String, SettableValue> consumer) {
			bindings.forEach(consumer);
		}

		Binding build(Table table, BindMarkers bindMarkers) {

			Map<BindMarker, Object> values = new LinkedHashMap<>();
			Map<BindMarker, SettableValue> nulls = new LinkedHashMap<>();

			AtomicReference<Condition> conditionRef = new AtomicReference<>();

			forEachFilter((k, v) -> {

				Condition condition = toCondition(bindMarkers, table.column(k), v, values, nulls);
				Condition current = conditionRef.get();
				if (current == null) {
					current = condition;
				} else {
					current = current.and(condition);
				}

				conditionRef.set(current);
			});

			return new Binding(values, nulls, conditionRef.get());

		}

		private static Condition toCondition(BindMarkers bindMarkers, Column column, SettableValue value,
				Map<BindMarker, Object> values, Map<BindMarker, SettableValue> nulls) {

			if (value.hasValue()) {

				Object bindValue = value.getValue();

				if (bindValue instanceof Iterable) {

					Iterable<?> iterable = (Iterable<?>) bindValue;
					List<Expression> expressions = new ArrayList<>();

					for (Object o : iterable) {
						BindMarker marker = bindMarkers.next(column.getName());
						if (o == null) {
							nulls.put(marker, value);
						} else {
							values.put(marker, o);
						}
						expressions.add(SQL.bindMarker(marker.getPlaceholder()));
					}

					return column.in(expressions.toArray(new Expression[0]));
				}

				BindMarker marker = bindMarkers.next(column.getName());
				values.put(marker, value.getValue());
				return column.isEqualTo(SQL.bindMarker(marker.getPlaceholder()));
			}

			return column.isNull();
		}
	}

	/**
	 * Value object holding value and {@code NULL} bindings.
	 *
	 * @see SettableValue
	 */
	@RequiredArgsConstructor
	@Getter
	static class Binding {

		private final Map<BindMarker, Object> values;
		private final Map<BindMarker, SettableValue> nulls;

		private final @Nullable Condition condition;

		boolean hasCondition() {
			return condition != null;
		}

		/**
		 * Append bindings.
		 *
		 * @param assignmentBindings
		 * @return
		 */
		Binding withBindings(Map<BindMarker, SettableValue> assignmentBindings) {

			assignmentBindings.forEach(((bindMarker, settableValue) -> {

				if (settableValue.isEmpty()) {
					nulls.put(bindMarker, settableValue);
				} else {
					values.put(bindMarker, settableValue.getValue());
				}
			}));

			return this;
		}

		/**
		 * Apply bindings to a {@link Statement}.
		 *
		 * @param to
		 */
		void apply(Statement to) {

			values.forEach((marker, value) -> marker.bind(to, value));
			nulls.forEach((marker, value) -> marker.bindNull(to, value.getType()));
		}
	}

	static abstract class PreparedOperationSupport<T> implements PreparedOperation<T> {

		private Function<String, String> sqlFilter = s -> s;
		private Function<Bindings, Bindings> bindingFilter = b -> b;
		private final Bindings bindings;

		protected PreparedOperationSupport(Bindings bindings) {

			this.bindings = bindings;
		}

		abstract protected String createBaseSql();

		Bindings getBaseBinding() {
			return bindings;
		};

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {

			return sqlFilter.apply(createBaseSql());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.PreparedOperation#bind(io.r2dbc.spi.Statement)
		 */
		protected Statement bind(Statement to) {

			bindingFilter.apply(getBaseBinding()).apply(to);
			return to;
		}

		@Override
		public Statement createBoundStatement(Connection connection) {

			// TODO add back logging
			// if (logger.isDebugEnabled()) {
			// logger.debug("Executing SQL statement [" + sql + "]");
			// }

			return bind(connection.createStatement(toQuery()));
		}

		@Override
		public void addSqlFilter(Function<String, String> filter) {

			Assert.notNull(filter, "Filter must not be null.");

			sqlFilter = filter;

		}

		@Override
		public void addBindingFilter(Function<Bindings, Bindings> filter) {

			Assert.notNull(filter, "Filter must not be null.");

			bindingFilter = filter;
		}

	}

	/**
	 * Default implementation of {@link PreparedOperation}.
	 *
	 * @param <T>
	 */
	static class DefaultPreparedOperation<T> extends PreparedOperationSupport<T> {

		private final T source;
		private final RenderContext renderContext;

		DefaultPreparedOperation(T source, RenderContext renderContext, Bindings bindings) {

			super(bindings);

			this.source = source;
			this.renderContext = renderContext;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.PreparedOperation#getSource()
		 */
		@Override
		public T getSource() {
			return this.source;
		}

		@Override
		protected String createBaseSql() {

			SqlRenderer sqlRenderer = SqlRenderer.create(renderContext);

			if (this.source instanceof Select) {
				return sqlRenderer.render((Select) this.source);
			}

			if (this.source instanceof Insert) {
				return sqlRenderer.render((Insert) this.source);
			}

			if (this.source instanceof Update) {
				return sqlRenderer.render((Update) this.source);
			}

			if (this.source instanceof Delete) {
				return sqlRenderer.render((Delete) this.source);
			}

			throw new IllegalStateException("Cannot render " + this.getSource());
		}

	}
}
