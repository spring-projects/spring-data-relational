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
package org.springframework.data.jdbc.mapping.model;

import static java.util.Arrays.*;

import lombok.Getter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * {@link MappingContext} implementation for JDBC.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Kazuki Shimizu
 * @since 2.0
 */
public class JdbcMappingContext extends AbstractMappingContext<JdbcPersistentEntity<?>, JdbcPersistentProperty> {

	private static final HashSet<Class<?>> CUSTOM_SIMPLE_TYPES = new HashSet<>(asList( //
			BigDecimal.class, //
			BigInteger.class, //
			Temporal.class //
	));

	@Getter private final NamingStrategy namingStrategy;
	@Getter private final NamedParameterJdbcOperations template;
	@Getter private SimpleTypeHolder simpleTypeHolder;
	private GenericConversionService conversions = getDefaultConversionService();

	public JdbcMappingContext(NamingStrategy namingStrategy, NamedParameterJdbcOperations template,
			ConversionCustomizer customizer) {

		this.namingStrategy = namingStrategy;
		this.template = template;

		customizer.customize(conversions);
		setSimpleTypeHolder(new SimpleTypeHolder(CUSTOM_SIMPLE_TYPES, true));
	}

	public JdbcMappingContext(NamedParameterJdbcOperations template) {
		this(NamingStrategy.INSTANCE, template, __ -> {});
	}

	@Override
	public void setSimpleTypeHolder(SimpleTypeHolder simpleTypes) {
		super.setSimpleTypeHolder(simpleTypes);
		this.simpleTypeHolder = simpleTypes;
	}

	public List<PropertyPath> referencedEntities(Class<?> rootType, PropertyPath path) {

		List<PropertyPath> paths = new ArrayList<>();

		Class<?> currentType = path == null ? rootType : path.getLeafType();
		JdbcPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(currentType);

		for (JdbcPersistentProperty property : persistentEntity) {
			if (property.isEntity()) {

				PropertyPath nextPath = path == null ? PropertyPath.from(property.getName(), rootType)
						: path.nested(property.getName());
				paths.add(nextPath);
				paths.addAll(referencedEntities(rootType, nextPath));
			}
		}

		Collections.reverse(paths);

		return paths;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> JdbcPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new JdbcPersistentEntityImpl<>(typeInformation, this.namingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentProperty(org.springframework.data.mapping.model.Property, org.springframework.data.mapping.model.MutablePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected JdbcPersistentProperty createPersistentProperty(Property property, JdbcPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {
		return new BasicJdbcPersistentProperty(property, owner, simpleTypeHolder, this);
	}

	@SuppressWarnings("unchecked")
	public <T> JdbcPersistentEntityInformation<T, ?> getRequiredPersistentEntityInformation(Class<T> type) {
		return new BasicJdbcPersistentEntityInformation<>((JdbcPersistentEntity<T>) getRequiredPersistentEntity(type));
	}

	public ConversionService getConversions() {
		return conversions;
	}

	private static GenericConversionService getDefaultConversionService() {

		DefaultConversionService conversionService = new DefaultConversionService();
		Jsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);

		return conversionService;
	}
}
