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
package org.springframework.data.r2dbc.mapping;

import org.springframework.core.KotlinDetector;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.util.KotlinReflectionUtils;
import org.springframework.data.util.TypeInformation;

/**
 * R2DBC-specific extension to {@link RelationalMappingContext}.
 *
 * @author Mark Paluch
 */
public class R2dbcMappingContext extends RelationalMappingContext {

	/**
	 * Create a new {@link R2dbcMappingContext}.
	 */
	public R2dbcMappingContext() {
		setForceQuote(false);
	}

	/**
	 * Create a new {@link R2dbcMappingContext} using the given {@link NamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public R2dbcMappingContext(NamingStrategy namingStrategy) {
		super(namingStrategy);
		setForceQuote(false);
	}

	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {

		if (R2dbcSimpleTypeHolder.HOLDER.isSimpleType(type.getType())) {
			return false;
		}

		return !KotlinDetector.isKotlinType(type.getType()) || KotlinReflectionUtils.isSupportedKotlinClass(type.getType());
	}
}
