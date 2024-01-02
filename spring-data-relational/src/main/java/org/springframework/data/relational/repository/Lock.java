/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.relational.repository;

import org.springframework.data.annotation.QueryAnnotation;
import org.springframework.data.relational.core.sql.LockMode;

import java.lang.annotation.*;

/**
 * Annotation to provide a lock mode for a given query.
 *
 * @author Diego Krupitza
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@QueryAnnotation
@Documented
public @interface Lock {

	/**
	 * Defines which type of {@link LockMode} we want to use.
	 */
	LockMode value();

}
