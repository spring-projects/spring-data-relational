/*
 * Copyright 2019-2024 the original author or authors.
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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The annotation to configure the mapping for a {@link List}, {@link Set} or {@link Map} property in the database.
 *
 * @since 1.1
 * @author Bastian Wilhelm
 * @author Mark Paluch
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface MappedCollection {

	/**
	 * The column name for id column in the corresponding relationship table. The attribute supports SpEL expressions to
	 * dynamically calculate the column name on a per-operation basis. Defaults to {@link NamingStrategy} usage if the
	 * value is empty.
	 *
	 * @see NamingStrategy#getReverseColumnName(RelationalPersistentProperty)
	 */
	String idColumn() default "";

	/**
	 * The column name for key columns of {@link List} or {@link Map} collections in the corresponding relationship table.
	 * The attribute supports SpEL expressions to dynamically calculate the column name on a per-operation basis. Defaults
	 * to {@link NamingStrategy} usage if the value is empty.
	 *
	 * @see NamingStrategy#getKeyColumn(RelationalPersistentProperty)
	 */
	String keyColumn() default "";
}
