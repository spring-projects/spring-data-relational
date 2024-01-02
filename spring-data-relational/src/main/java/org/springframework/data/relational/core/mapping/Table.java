/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * The annotation to configure the mapping from a class to a database table.
 *
 * @author Kazuki Shimizu
 * @author Bastian Wilhelm
 * @author Mikhail Polivakha
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Inherited
public @interface Table {

	/**
	 * The table name. The attribute supports SpEL expressions to dynamically calculate the table name on a per-operation
	 * basis.
	 */
	@AliasFor("name")
	String value() default "";

	/**
	 * The table name. The attribute supports SpEL expressions to dynamically calculate the table name on a per-operation
	 * basis.
	 */
	@AliasFor("value")
	String name() default "";

	/**
	 * Name of the schema (or user, for example in case of oracle), in which this table resides in The behavior is the
	 * following: <br/>
	 * If the {@link Table#schema()} is specified, then it will be used as a schema of current table, i.e. as a prefix to
	 * the name of the table, which can be specified in {@link Table#value()}. <br/>
	 * If the {@link Table#schema()} is not specified, then spring data will assume the default schema, The default schema
	 * itself can be provided by the means of {@link NamingStrategy#getSchema()}
	 */
	String schema() default "";
}
