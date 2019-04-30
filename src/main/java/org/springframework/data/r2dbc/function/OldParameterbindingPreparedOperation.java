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

import java.util.Map;
import java.util.function.Function;

import org.springframework.data.r2dbc.domain.SettableValue;

/**
 * @author Jens Schauder
 */
public class OldParameterbindingPreparedOperation implements PreparedOperation<BindableOperation> {

	private final BindableOperation operation;
	private final Map<String, SettableValue> byName;
	private final Map<Integer, SettableValue> byIndex;

	private OldParameterbindingPreparedOperation(BindableOperation operation, Map<String, SettableValue> byName,
												 Map<Integer, SettableValue> byIndex) {

		this.operation = operation;
		this.byName = byName;
		this.byIndex = byIndex;
	}

	OldParameterbindingPreparedOperation(String sql, NamedParameterExpander namedParameters,
										 ReactiveDataAccessStrategy dataAccessStrategy, Map<String, SettableValue> byName,
										 Map<Integer, SettableValue> byIndex) {

		this( //
				namedParameters.expand(sql, dataAccessStrategy.getBindMarkersFactory(), new MapBindParameterSource(byName)), //
				byName, //
				byIndex //
		);
	}

	@Override
	public BindableOperation getSource() {
		return operation;
	}

	@Override
	public Statement createBoundStatement(Connection connection) {

		Statement statement = connection.createStatement(operation.toQuery());

		bindByName(statement, byName);
		bindByIndex(statement, byIndex);

		return statement;
	}

	@Override
	public void addSqlFilter(Function<String, String> filter) {

	}

	@Override
	public void addBindingFilter(Function<Bindings, Bindings> filter) {

	}

	@Override
	public String toQuery() {
		return operation.toQuery();
	}

	// todo that is a weird assymmetry between bindByName and bindByIndex
	private  void bindByName(Statement statement, Map<String, SettableValue> byName) {

		byName.forEach((name, o) -> {

			if (o.getValue() != null) {
				operation.bind(statement,name, o.getValue());
			} else {
				operation.bindNull(statement, name, o.getType());
			}
		});
	}

	private static void bindByIndex(Statement statement, Map<Integer, SettableValue> byIndex) {

		byIndex.forEach((i, o) -> {

			if (o.getValue() != null) {
				statement.bind(i.intValue(), o.getValue());
			} else {
				statement.bindNull(i.intValue(), o.getType());
			}
		});
	}
}
