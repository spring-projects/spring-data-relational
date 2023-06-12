/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import org.springframework.data.util.Lazy;

/**
 * A wrapper for the {@link SqlGenerator} that caches the generated statements.
 * @since 3.2
 * @author Jens Schauder
 */
public class CachingSqlGenerator implements SqlGenerator{

	private final SqlGenerator delegate;

	private final Lazy<String> findAll;
	private final Lazy<String> findById;
	private final Lazy<String> findAllById;

	public CachingSqlGenerator(SqlGenerator delegate) {

		this.delegate = delegate;

		findAll = Lazy.of(delegate.findAll());
		findById = Lazy.of(delegate.findById());
		findAllById = Lazy.of(delegate.findAllById());
	}

	@Override
	public String findAll() {
		return findAll.get();
	}

	@Override
	public String findById() {
		return findById.get();
	}

	@Override
	public String findAllById() {
		return findAllById.get();
	}

	@Override
	public AliasFactory getAliasFactory() {
		return delegate.getAliasFactory();
	}
}
