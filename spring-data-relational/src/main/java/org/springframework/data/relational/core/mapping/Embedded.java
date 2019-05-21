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
package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation to configure a value object as embedded in the current table.
 * <p />
 * Depending on the {@link OnEmpty value} of {@link #onEmpty()} the property is set to {@literal null} or an empty
 * instance in the case all embedded values are {@literal null} when reading from the result set.
 *
 * @author Bastian Wilhelm
 * @author Christoph Strobl
 * @since 1.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface Embedded {

	/**
	 * Set the load strategy for the embedded object if all contained fields yield {@literal null} values.
	 * 
	 * @return never {@link} null.
	 */
	OnEmpty onEmpty();

	/**
	 * @return prefix for columns in the embedded value object. An empty {@link String} by default.
	 */
	String prefix() default "";

	/**
	 * Load strategy to be used {@link Embedded#onEmpty()}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.1
	 */
	enum OnEmpty {
		USE_NULL, USE_EMPTY
	}
}
