/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.Update;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * A non-blocking, reactive client for performing database calls requests with Reactive Streams back pressure. Provides
 * a higher level, common API over R2DBC client libraries.
 * <p>
 * Use one of the static factory methods {@link #create(ConnectionFactory)} or obtain a {@link DatabaseClient#builder()}
 * to create an instance.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 * @deprecated since 1.2, use Spring R2DBC's {@link org.springframework.r2dbc.core.DatabaseClient} support instead.
 */
@Deprecated
public interface DatabaseClient {

	/**
	 * Specify a static {@code sql} string to execute. Contract for specifying a SQL call along with options leading to
	 * the exchange. The SQL string can contain either native parameter bind markers or named parameters (e.g.
	 * {@literal :foo, :bar}) when {@link NamedParameterExpander} is enabled.
	 *
	 * @param sql must not be {@literal null} or empty.
	 * @return a new {@link GenericExecuteSpec}.
	 * @see NamedParameterExpander
	 * @see DatabaseClient.Builder#namedParameters(boolean)
	 */
	GenericExecuteSpec execute(String sql);

	/**
	 * Specify a {@link Supplier SQL supplier} that provides SQL to execute. Contract for specifying a SQL call along with
	 * options leading to the exchange. The SQL string can contain either native parameter bind markers or named
	 * parameters (e.g. {@literal :foo, :bar}) when {@link NamedParameterExpander} is enabled.
	 * <p>
	 * Accepts {@link PreparedOperation} as SQL and binding {@link Supplier}.
	 * </p>
	 *
	 * @param sqlSupplier must not be {@literal null}.
	 * @return a new {@link GenericExecuteSpec}.
	 * @see NamedParameterExpander
	 * @see DatabaseClient.Builder#namedParameters(boolean)
	 * @see PreparedOperation
	 */
	GenericExecuteSpec execute(Supplier<String> sqlSupplier);

	/**
	 * Prepare an SQL SELECT call.
	 */
	SelectFromSpec select();

	/**
	 * Prepare an SQL INSERT call.
	 */
	InsertIntoSpec insert();

	/**
	 * Prepare an SQL UPDATE call.
	 */
	UpdateTableSpec update();

	/**
	 * Prepare an SQL DELETE call.
	 */
	DeleteFromSpec delete();

	/**
	 * Return a builder to mutate properties of this database client.
	 */
	DatabaseClient.Builder mutate();

	// Static, factory methods

	/**
	 * Creates a {@code DatabaseClient} that will use the provided {@link io.r2dbc.spi.ConnectionFactory}.
	 *
	 * @param factory The {@code ConnectionFactory} to use for obtaining connections.
	 * @return a new {@code DatabaseClient}. Guaranteed to be not {@literal null}.
	 */
	static DatabaseClient create(ConnectionFactory factory) {
		return new DefaultDatabaseClientBuilder().connectionFactory(factory).build();
	}

	/**
	 * Obtain a {@code DatabaseClient} builder.
	 */
	static DatabaseClient.Builder builder() {
		return new DefaultDatabaseClientBuilder();
	}

	/**
	 * A mutable builder for creating a {@link DatabaseClient}.
	 */
	interface Builder {

		/**
		 * Configures the {@link ConnectionFactory R2DBC connector}.
		 *
		 * @param factory must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder connectionFactory(ConnectionFactory factory);

		/**
		 * Configures a {@link R2dbcExceptionTranslator}.
		 *
		 * @param exceptionTranslator must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder exceptionTranslator(R2dbcExceptionTranslator exceptionTranslator);

		/**
		 * Configures a {@link ExecuteFunction} to execute {@link Statement} objects.
		 *
		 * @param executeFunction must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 * @since 1.1
		 * @see Statement#execute()
		 */
		Builder executeFunction(ExecuteFunction executeFunction);

		/**
		 * Configures a {@link ReactiveDataAccessStrategy}.
		 *
		 * @param accessStrategy must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder dataAccessStrategy(ReactiveDataAccessStrategy accessStrategy);

		/**
		 * Configures whether to use named parameter expansion. Defaults to {@literal true}.
		 *
		 * @param enabled {@literal true} to use named parameter expansion. {@literal false} to disable named parameter
		 *          expansion.
		 * @return {@code this} {@link Builder}.
		 * @see NamedParameterExpander
		 */
		Builder namedParameters(boolean enabled);

		/**
		 * Configures the {@link org.springframework.data.projection.ProjectionFactory projection factory}.
		 *
		 * @param factory must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 * @since 1.1
		 */
		Builder projectionFactory(ProjectionFactory factory);

		/**
		 * Configures a {@link Consumer} to configure this builder.
		 *
		 * @param builderConsumer must not be {@literal null}.
		 * @return {@code this} {@link Builder}.
		 */
		Builder apply(Consumer<Builder> builderConsumer);

		/**
		 * Builder the {@link DatabaseClient} instance.
		 */
		DatabaseClient build();
	}

	/**
	 * Contract for specifying a SQL call along with options leading to the exchange.
	 */
	interface GenericExecuteSpec extends BindSpec<GenericExecuteSpec>, StatementFilterSpec<GenericExecuteSpec> {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 */
		<R> TypedExecuteSpec<R> as(Class<R> resultType);

		/**
		 * Configure a result mapping {@link java.util.function.Function function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(Function<Row, R> mappingFunction);

		/**
		 * Configure a result mapping {@link java.util.function.BiFunction function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		FetchSpec<Map<String, Object>> fetch();

		/**
		 * Perform the SQL call and return a {@link Mono} that completes without result on statement completion.
		 *
		 * @return a {@link Mono} ignoring its payload (actively dropping).
		 */
		Mono<Void> then();
	}

	/**
	 * Contract for specifying a SQL call along with options leading to the exchange.
	 */
	interface TypedExecuteSpec<T> extends BindSpec<TypedExecuteSpec<T>>, StatementFilterSpec<TypedExecuteSpec<T>> {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 */
		<R> TypedExecuteSpec<R> as(Class<R> resultType);

		/**
		 * Configure a result mapping {@link java.util.function.Function function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(Function<Row, R> mappingFunction);

		/**
		 * Configure a result mapping {@link java.util.function.BiFunction function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		FetchSpec<T> fetch();

		/**
		 * Perform the SQL call and return a {@link Mono} that completes without result on statement completion.
		 *
		 * @return a {@link Mono} ignoring its payload (actively dropping).
		 */
		Mono<Void> then();
	}

	/**
	 * Contract for specifying {@code SELECT} options leading to the exchange.
	 */
	interface SelectFromSpec {

		/**
		 * Specify the source {@code table} to select from.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericSelectSpec} for further configuration of the select. Guaranteed to be not
		 *         {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default GenericSelectSpec from(String table) {
			return from(SqlIdentifier.unquoted(table));
		}

		/**
		 * Specify the source {@code table} to select from.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericSelectSpec} for further configuration of the select. Guaranteed to be not
		 *         {@literal null}.
		 * @since 1.1
		 */
		GenericSelectSpec from(SqlIdentifier table);

		/**
		 * Specify the source table to select from to using the {@link Class entity class}.
		 *
		 * @param table must not be {@literal null}.
		 * @return a {@link TypedSelectSpec} for further configuration of the select. Guaranteed to be not {@literal null}.
		 */
		<T> TypedSelectSpec<T> from(Class<T> table);
	}

	/**
	 * Contract for specifying {@code INSERT} options leading to the exchange.
	 */
	interface InsertIntoSpec {

		/**
		 * Specify the target {@code table} to insert into.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericInsertSpec} for further configuration of the insert. Guaranteed to be not
		 *         {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default GenericInsertSpec<Map<String, Object>> into(String table) {
			return into(SqlIdentifier.unquoted(table));
		}

		/**
		 * Specify the target {@code table} to insert into.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericInsertSpec} for further configuration of the insert. Guaranteed to be not
		 *         {@literal null}.
		 * @since 1.1
		 */
		GenericInsertSpec<Map<String, Object>> into(SqlIdentifier table);

		/**
		 * Specify the target table to insert to using the {@link Class entity class}.
		 *
		 * @param table must not be {@literal null}.
		 * @return a {@link TypedInsertSpec} for further configuration of the insert. Guaranteed to be not {@literal null}.
		 */
		<T> TypedInsertSpec<T> into(Class<T> table);
	}

	/**
	 * Contract for specifying {@code UPDATE} options leading to the exchange.
	 */
	interface UpdateTableSpec {

		/**
		 * Specify the target {@code table} to update.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericUpdateSpec} for further configuration of the update. Guaranteed to be not
		 *         {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default GenericUpdateSpec table(String table) {
			return table(SqlIdentifier.unquoted(table));
		}

		/**
		 * Specify the target {@code table} to update.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link GenericUpdateSpec} for further configuration of the update. Guaranteed to be not
		 *         {@literal null}.
		 * @since 1.1
		 */
		GenericUpdateSpec table(SqlIdentifier table);

		/**
		 * Specify the target table to update to using the {@link Class entity class}.
		 *
		 * @param table must not be {@literal null}.
		 * @return a {@link TypedUpdateSpec} for further configuration of the update. Guaranteed to be not {@literal null}.
		 */
		<T> TypedUpdateSpec<T> table(Class<T> table);
	}

	/**
	 * Contract for specifying {@code DELETE} options leading to the exchange.
	 */
	interface DeleteFromSpec {

		/**
		 * Specify the source {@code table} to delete from.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link DeleteMatchingSpec} for further configuration of the delete. Guaranteed to be not
		 *         {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default DeleteMatchingSpec from(String table) {
			return from(SqlIdentifier.unquoted(table));
		}

		/**
		 * Specify the source {@code table} to delete from.
		 *
		 * @param table must not be {@literal null} or empty.
		 * @return a {@link DeleteMatchingSpec} for further configuration of the delete. Guaranteed to be not
		 *         {@literal null}.
		 * @since 1.1
		 */
		DeleteMatchingSpec from(SqlIdentifier table);

		/**
		 * Specify the source table to delete from to using the {@link Class entity class}.
		 *
		 * @param table must not be {@literal null}.
		 * @return a {@link TypedDeleteSpec} for further configuration of the delete. Guaranteed to be not {@literal null}.
		 */
		<T> TypedDeleteSpec<T> from(Class<T> table);
	}

	/**
	 * Contract for specifying {@code SELECT} options leading to the exchange.
	 */
	interface GenericSelectSpec extends SelectSpec<GenericSelectSpec> {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 */
		<R> TypedSelectSpec<R> as(Class<R> resultType);

		/**
		 * Configure a result mapping {@link java.util.function.Function function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(Function<Row, R> mappingFunction);

		/**
		 * Configure a result mapping {@link java.util.function.BiFunction function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		FetchSpec<Map<String, Object>> fetch();
	}

	/**
	 * Contract for specifying {@code SELECT} options leading to the exchange.
	 */
	interface TypedSelectSpec<T> extends SelectSpec<TypedSelectSpec<T>> {

		/**
		 * Define the target type the result should be mapped to. <br />
		 * Skip this step if you are anyway fine with the default conversion.
		 *
		 * @param resultType must not be {@literal null}.
		 * @param <R> result type.
		 */
		<R> RowsFetchSpec<R> as(Class<R> resultType);

		/**
		 * Configure a result mapping {@link java.util.function.Function function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(Function<Row, R> mappingFunction);

		/**
		 * Configure a result mapping {@link java.util.function.BiFunction function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		FetchSpec<T> fetch();
	}

	/**
	 * Contract for specifying {@code SELECT} options leading to the exchange.
	 */
	interface SelectSpec<S extends SelectSpec<S>> {

		/**
		 * Configure projected fields.
		 *
		 * @param selectedFields must not be {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default S project(String... selectedFields) {
			return project(Arrays.stream(selectedFields).map(SqlIdentifier::unquoted).toArray(SqlIdentifier[]::new));
		}

		/**
		 * Configure projected fields.
		 *
		 * @param selectedFields must not be {@literal null}.
		 * @since 1.1
		 */
		S project(SqlIdentifier... selectedFields);

		/**
		 * Configure a filter {@link CriteriaDefinition}.
		 *
		 * @param criteria must not be {@literal null}.
		 */
		S matching(CriteriaDefinition criteria);

		/**
		 * Configure {@link Sort}.
		 *
		 * @param sort must not be {@literal null}.
		 */
		S orderBy(Sort sort);

		/**
		 * Configure {@link Sort}.
		 *
		 * @param orders must not be {@literal null}.
		 */
		default S orderBy(Sort.Order... orders) {
			return orderBy(Sort.by(orders));
		}

		/**
		 * Configure pagination. Overrides {@link Sort} if the {@link Pageable} contains a {@link Sort} object.
		 *
		 * @param pageable must not be {@literal null}.
		 */
		S page(Pageable pageable);
	}

	/**
	 * Contract for specifying {@code INSERT} options leading to the exchange.
	 *
	 * @param <T> Result type of tabular insert results.
	 */
	interface GenericInsertSpec<T> extends InsertSpec<T> {

		/**
		 * Specify a field and non-{@literal null} value to insert. {@code value} can be either a scalar value or
		 * {@link SettableValue}.
		 *
		 * @param field must not be {@literal null} or empty.
		 * @param value the field value to set, must not be {@literal null}. Can be either a scalar value or
		 *          {@link SettableValue}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default GenericInsertSpec<T> value(String field, Object value) {
			return value(SqlIdentifier.unquoted(field), value);
		}

		/**
		 * Specify a field and non-{@literal null} value to insert. {@code value} can be either a scalar value or
		 * {@link SettableValue}.
		 *
		 * @param field must not be {@literal null} or empty.
		 * @param value the field value to set, must not be {@literal null}. Can be either a scalar value or
		 *          {@link SettableValue}.
		 */
		GenericInsertSpec<T> value(SqlIdentifier field, Object value);

		/**
		 * Specify a {@literal null} value to insert.
		 *
		 * @param field must not be {@literal null} or empty.
		 * @param type must not be {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default GenericInsertSpec<T> nullValue(String field, Class<?> type) {
			return nullValue(SqlIdentifier.unquoted(field), type);
		}

		/**
		 * Specify a {@literal null} value to insert.
		 *
		 * @param field must not be {@literal null} or empty.
		 * @param type must not be {@literal null}.
		 * @since 1.1
		 */
		default GenericInsertSpec<T> nullValue(SqlIdentifier field, Class<?> type) {
			return value(field, SettableValue.empty(type));
		}
	}

	/**
	 * Contract for specifying {@code INSERT} options leading the exchange.
	 */
	interface TypedInsertSpec<T> {

		/**
		 * Insert the given {@code objectToInsert}.
		 *
		 * @param objectToInsert the object of which the attributes will provide the values for the insert. Must not be
		 *          {@literal null}.
		 * @return a {@link InsertSpec} for further configuration of the insert. Guaranteed to be not {@literal null}.
		 */
		InsertSpec<Map<String, Object>> using(T objectToInsert);

		/**
		 * Use the given {@code tableName} as insert target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedInsertSpec} for further configuration of the insert. Guaranteed to be not {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default TypedInsertSpec<T> table(String tableName) {
			return table(SqlIdentifier.unquoted(tableName));
		}

		/**
		 * Use the given {@code tableName} as insert target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedInsertSpec} for further configuration of the insert. Guaranteed to be not {@literal null}.
		 * @since 1.1
		 */
		TypedInsertSpec<T> table(SqlIdentifier tableName);

		/**
		 * Insert the given {@link Publisher} to insert one or more objects. Inserts only a single object when calling
		 * {@link FetchSpec#one()} or {@link FetchSpec#first()}.
		 *
		 * @param objectToInsert a publisher providing the objects of which the attributes will provide the values for the
		 *          insert. Must not be {@literal null}.
		 * @return a {@link InsertSpec} for further configuration of the insert. Guaranteed to be not {@literal null}.
		 * @see InsertSpec#fetch()
		 */
		InsertSpec<Map<String, Object>> using(Publisher<T> objectToInsert);
	}

	/**
	 * Contract for specifying {@code INSERT} options leading to the exchange.
	 *
	 * @param <T> Result type of tabular insert results.
	 */
	interface InsertSpec<T> {

		/**
		 * Configure a result mapping {@link java.util.function.Function function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(Function<Row, R> mappingFunction);

		/**
		 * Configure a result mapping {@link java.util.function.BiFunction function}.
		 *
		 * @param mappingFunction must not be {@literal null}.
		 * @param <R> result type.
		 * @return a {@link FetchSpec} for configuration what to fetch. Guaranteed to be not {@literal null}.
		 */
		<R> RowsFetchSpec<R> map(BiFunction<Row, RowMetadata, R> mappingFunction);

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		FetchSpec<T> fetch();

		/**
		 * Perform the SQL call and return a {@link Mono} that completes without result on statement completion.
		 *
		 * @return a {@link Mono} ignoring its payload (actively dropping).
		 */
		Mono<Void> then();
	}

	/**
	 * Contract for specifying {@code UPDATE} options leading to the exchange.
	 */
	interface GenericUpdateSpec {

		/**
		 * Specify an {@link Update} object containing assignments.
		 *
		 * @param update must not be {@literal null}.
		 * @deprecated since 1.1, use {@link #using(org.springframework.data.relational.core.query.Update)}.
		 */
		@Deprecated
		UpdateMatchingSpec using(Update update);

		/**
		 * Specify an {@link Update} object containing assignments.
		 *
		 * @param update must not be {@literal null}.
		 * @since 1.1
		 */
		UpdateMatchingSpec using(org.springframework.data.relational.core.query.Update update);
	}

	/**
	 * Contract for specifying {@code UPDATE} options leading to the exchange.
	 */
	interface TypedUpdateSpec<T> {

		/**
		 * Update the given {@code objectToUpdate}.
		 *
		 * @param objectToUpdate the object of which the attributes will provide the values for the update and the primary
		 *          key. Must not be {@literal null}.
		 * @return a {@link UpdateMatchingSpec} for further configuration of the update. Guaranteed to be not {@literal null}.
		 */
		UpdateMatchingSpec using(T objectToUpdate);

		/**
		 * Use the given {@code tableName} as update target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedUpdateSpec} for further configuration of the update. Guaranteed to be not {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default TypedUpdateSpec<T> table(String tableName) {
			return table(SqlIdentifier.unquoted(tableName));
		}

		/**
		 * Use the given {@code tableName} as update target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedUpdateSpec} for further configuration of the update. Guaranteed to be not {@literal null}.
		 * @since 1.1
		 */
		TypedUpdateSpec<T> table(SqlIdentifier tableName);
	}

	/**
	 * Contract for specifying {@code UPDATE} options leading to the exchange.
	 */
	interface UpdateMatchingSpec extends UpdateSpec {

		/**
		 * Configure a filter {@link CriteriaDefinition}.
		 *
		 * @param criteria must not be {@literal null}.
		 */
		UpdateSpec matching(CriteriaDefinition criteria);
	}

	/**
	 * Contract for specifying {@code UPDATE} options leading to the exchange.
	 */
	interface UpdateSpec {

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		UpdatedRowsFetchSpec fetch();

		/**
		 * Perform the SQL call and return a {@link Mono} that completes without result on statement completion.
		 *
		 * @return a {@link Mono} ignoring its payload (actively dropping).
		 */
		Mono<Void> then();
	}

	/**
	 * Contract for specifying {@code DELETE} options leading to the exchange.
	 */
	interface TypedDeleteSpec<T> extends DeleteSpec {

		/**
		 * Use the given {@code tableName} as delete target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedDeleteSpec} for further configuration of the delete. Guaranteed to be not {@literal null}.
		 * @see SqlIdentifier#unquoted(String)
		 */
		default TypedDeleteSpec<T> table(String tableName) {
			return table(SqlIdentifier.unquoted(tableName));
		}

		/**
		 * Use the given {@code tableName} as delete target.
		 *
		 * @param tableName must not be {@literal null} or empty.
		 * @return a {@link TypedDeleteSpec} for further configuration of the delete. Guaranteed to be not {@literal null}.
		 * @since 1.1
		 */
		TypedDeleteSpec<T> table(SqlIdentifier tableName);

		/**
		 * Configure a filter {@link CriteriaDefinition}.
		 *
		 * @param criteria must not be {@literal null}.
		 */
		DeleteSpec matching(CriteriaDefinition criteria);
	}

	/**
	 * Contract for specifying {@code DELETE} options leading to the exchange.
	 */
	interface DeleteMatchingSpec extends DeleteSpec {

		/**
		 * Configure a filter {@link CriteriaDefinition}.
		 *
		 * @param criteria must not be {@literal null}.
		 */
		DeleteSpec matching(CriteriaDefinition criteria);
	}

	/**
	 * Contract for specifying {@code DELETE} options leading to the exchange.
	 */
	interface DeleteSpec {

		/**
		 * Perform the SQL call and retrieve the result.
		 */
		UpdatedRowsFetchSpec fetch();

		/**
		 * Perform the SQL call and return a {@link Mono} that completes without result on statement completion.
		 *
		 * @return a {@link Mono} ignoring its payload (actively dropping).
		 */
		Mono<Void> then();
	}

	/**
	 * Contract for specifying parameter bindings.
	 */
	interface BindSpec<S extends BindSpec<S>> {

		/**
		 * Bind a non-{@literal null} value to a parameter identified by its {@code index}. {@code value} can be either a
		 * scalar value or {@link SettableValue}.
		 *
		 * @param index zero based index to bind the parameter to.
		 * @param value must not be {@literal null}. Can be either a scalar value or {@link SettableValue}.
		 */
		S bind(int index, Object value);

		/**
		 * Bind a {@literal null} value to a parameter identified by its {@code index}.
		 *
		 * @param index zero based index to bind the parameter to.
		 * @param type must not be {@literal null}.
		 */
		S bindNull(int index, Class<?> type);

		/**
		 * Bind a non-{@literal null} value to a parameter identified by its {@code name}. {@code value} can be either a
		 * scalar value or {@link SettableValue}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param value must not be {@literal null}. Can be either a scalar value or {@link SettableValue}.
		 */
		S bind(String name, Object value);

		/**
		 * Bind a {@literal null} value to a parameter identified by its {@code name}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param type must not be {@literal null}.
		 */
		S bindNull(String name, Class<?> type);
	}

	/**
	 * Contract for applying a {@link StatementFilterFunction}.
	 *
	 * @since 1.1
	 */
	interface StatementFilterSpec<S extends StatementFilterSpec<S>> {

		/**
		 * Add the given filter to the end of the filter chain.
		 *
		 * @param filter the filter to be added to the chain.
		 */
		default S filter(Function<? super Statement, ? extends Statement> filter) {

			Assert.notNull(filter, "Statement FilterFunction must not be null!");

			return filter((statement, next) -> next.execute(filter.apply(statement)));
		}

		/**
		 * Add the given filter to the end of the filter chain.
		 *
		 * @param filter the filter to be added to the chain.
		 */
		S filter(StatementFilterFunction filter);
	}
}
