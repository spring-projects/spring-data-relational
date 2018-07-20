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

import org.springframework.lang.Nullable;

/**
 * {@link MyBatisContext} instances get passed to MyBatis mapped statements as arguments, making Ids, instances,
 * domainType and other attributes available to the statements. All methods might return {@literal null} depending on
 * the kind of values available on invocation.
 *
 * @author Jens Schauder
 */
public class MyBatisContext {

	private final Object id;
	private final Object instance;
	private final Class domainType;
	private final Map<String, Object> additonalValues;

	public MyBatisContext(@Nullable Object id, @Nullable Object instance, Class domainType, Map<String, Object> additonalValues) {

		this.id = id;
		this.instance = instance;
		this.domainType = domainType;
		this.additonalValues = additonalValues;
	}

	/**
	 * The ID of the entity to query/act upon.
	 *
	 * @return Might return {@code null}.
	 */
	@Nullable
	public Object getId() {
		return id;
	}

	/**
	 * The entity to act upon. This is {@code null} for queries, since the object doesn't exist before the query.
	 * 
	 * @return Might return {@code null}.
	 */
	@Nullable
	public Object getInstance() {
		return instance;
	}

	/**
	 * The domain type of the entity to query or act upon.
	 *
	 * @return Might return {@code null}.
	 */
	@Nullable
	public Class getDomainType() {
		return domainType;
	}

	/**
	 * Returns a value for the given key. Used to communicate ids of parent entities.
	 *
	 * @param key Must not be {@code null}.
	 * @return Might return {@code null}.
	 */
	@Nullable
	public Object get(String key) {
		return additonalValues.get(key);
	}
}
