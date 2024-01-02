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
package org.springframework.data.jdbc.mybatis;

/**
 * A strategy to derive a MyBatis namespace from a domainType.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 */
public interface NamespaceStrategy {

	NamespaceStrategy DEFAULT_INSTANCE = new NamespaceStrategy() {};

	/**
	 * Get a namespace that corresponds to the given domain type.
	 * <p>
	 * By default, the namespace is based on the class of the entity plus the suffix "Mapper".
	 *
	 * @param domainType Must be non {@literal null}.
	 * @return a namespace that correspond domain type
	 */
	default String getNamespace(Class<?> domainType) {
		return domainType.getName() + "Mapper";
	}

}
