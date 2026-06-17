/*
 * Copyright 2026-present the original author or authors.
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

import org.apache.ibatis.type.Alias;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

/**
 * Entity with an optimistic-locking version, used to exercise
 * {@link MyBatisDataAccessStrategy#deleteWithVersion(Object, Class, Number)}.
 *
 * @author Jens Schauder
 */
@Alias("VersionedEntity")
class VersionedEntity {

	@Id final Long id;
	@Version final Long version;
	final String name;

	public VersionedEntity(Long id, Long version, String name) {

		this.id = id;
		this.version = version;
		this.name = name;
	}
}
