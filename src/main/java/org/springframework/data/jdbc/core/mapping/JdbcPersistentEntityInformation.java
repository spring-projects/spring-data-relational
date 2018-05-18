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
package org.springframework.data.jdbc.core.mapping;

import org.springframework.data.repository.core.EntityInformation;

/**
 * @author Jens Schauder
 * @since 1.0
 */
public interface JdbcPersistentEntityInformation<T, ID> extends EntityInformation<T, ID> {

	void setId(T instance, Object value);

	/**
	 * Returns the identifier of the given entity or throws and exception if it can't be obtained.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the identifier of the given entity
	 * @throws IllegalArgumentException in case no identifier can be obtained for the given entity.
	 */
	default ID getRequiredId(T entity) {

		ID id = getId(entity);
		if (id == null)
			throw new IllegalStateException(String.format("Could not obtain required identifier from entity %s!", entity));

		return id;
	}
}
