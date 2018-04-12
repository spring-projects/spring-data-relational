/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.mybatis;

import java.util.Map;

/**
 * {@link MyBatisContext} instances get passed to MyBatis mapped statements as arguments, making Ids, instances, domainType and other attributes available to the statements.
 *
 * All methods might return {@literal null} depending on the kind of values available on invocation.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class MyBatisContext {

	private final Object id;
	private final Object instance;
	private final Class domainType;
	private final Map<String, Object> additonalValues;

	public MyBatisContext(Object id, Object instance, Class domainType, Map<String, Object> additonalValues) {

		this.id = id;
		this.instance = instance;
		this.domainType = domainType;
		this.additonalValues = additonalValues;
	}

	public Object getId() {
		return id;
	}

	public Object getInstance() {
		return instance;
	}

	public Class getDomainType() {
		return domainType;
	}

	public Object get(String key) {
		return additonalValues.get(key);
	}
}
