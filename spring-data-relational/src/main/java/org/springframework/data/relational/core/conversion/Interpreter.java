/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import org.springframework.data.relational.core.conversion.DbAction.Delete;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAll;
import org.springframework.data.relational.core.conversion.DbAction.DeleteAllRoot;
import org.springframework.data.relational.core.conversion.DbAction.DeleteRoot;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.conversion.DbAction.Merge;
import org.springframework.data.relational.core.conversion.DbAction.Update;
import org.springframework.data.relational.core.conversion.DbAction.UpdateRoot;

/**
 * An {@link Interpreter} gets called by a {@link AggregateChange} for each {@link DbAction} and is tasked with
 * executing that action against a database. While the {@link DbAction} is just an abstract representation of a database
 * action it's the task of an interpreter to actually execute it. This typically involves creating some SQL and running
 * it using JDBC, but it may also use some third party technology like MyBatis or jOOQ to do this.
 *
 * @author Jens Schauder
 */
public interface Interpreter {

	<T> void interpret(Insert<T> insert);

	<T> void interpret(InsertRoot<T> insert);

	/**
	 * Interpret an {@link Update}. Interpreting normally means "executing".
	 *
	 * @param <T> the type of entity to work on.
	 * @param update the {@link Update} to be executed
	 */
	<T> void interpret(Update<T> update);

	<T> void interpret(UpdateRoot<T> update);

	<T> void interpret(Merge<T> update);

	<T> void interpret(Delete<T> delete);

	<T> void interpret(DeleteRoot<T> deleteRoot);

	<T> void interpret(DeleteAll<T> delete);

	<T> void interpret(DeleteAllRoot<T> DeleteAllRoot);
}
