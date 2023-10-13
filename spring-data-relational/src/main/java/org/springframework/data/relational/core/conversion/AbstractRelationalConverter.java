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
package org.springframework.data.relational.core.conversion;

import java.util.Collections;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.util.Assert;

/**
 * Base class for {@link RelationalConverter} implementations. Sets up a {@link ConfigurableConversionService} and
 * populates basic converters. Allows registering {@link CustomConversions}.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public abstract class AbstractRelationalConverter implements RelationalConverter {

	private final RelationalMappingContext context;
	private final ConfigurableConversionService conversionService;
	private final EntityInstantiators entityInstantiators;
	private final CustomConversions conversions;

	/**
	 * Creates a new {@link AbstractRelationalConverter} given {@link MappingContext}.
	 *
	 * @param context must not be {@literal null}.
	 */
	public AbstractRelationalConverter(RelationalMappingContext context) {
		this(context, new CustomConversions(StoreConversions.NONE, Collections.emptyList()), new DefaultConversionService(),
				new EntityInstantiators());
	}

	/**
	 * Creates a new {@link AbstractRelationalConverter} given {@link MappingContext} and {@link CustomConversions}.
	 *
	 * @param context must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 */
	public AbstractRelationalConverter(RelationalMappingContext context, CustomConversions conversions) {
		this(context, conversions, new DefaultConversionService(), new EntityInstantiators());
	}

	private AbstractRelationalConverter(RelationalMappingContext context, CustomConversions conversions,
			ConfigurableConversionService conversionService, EntityInstantiators entityInstantiators) {

		Assert.notNull(context, "MappingContext must not be null");
		Assert.notNull(conversions, "CustomConversions must not be null");

		this.context = context;
		this.conversionService = conversionService;
		this.entityInstantiators = entityInstantiators;
		this.conversions = conversions;

		conversions.registerConvertersIn(this.conversionService);
	}

	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}

	public CustomConversions getConversions() {
		return conversions;
	}

	@Override
	public EntityInstantiators getEntityInstantiators() {
		return entityInstantiators;
	}

	@Override
	public RelationalMappingContext getMappingContext() {
		return context;
	}

}
