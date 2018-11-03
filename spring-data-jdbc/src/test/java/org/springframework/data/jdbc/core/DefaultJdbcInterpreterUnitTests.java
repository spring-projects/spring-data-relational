/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import java.util.AbstractMap.SimpleEntry;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.InsertRoot;
import org.springframework.data.relational.core.conversion.EffectiveParentId;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for {@link DefaultJdbcInterpreter}
 *
 * @author Jens Schauder
 */
public class DefaultJdbcInterpreterUnitTests {

	static final long CONTAINER_ID = 23L;
	static final String BACK_REFERENCE = "back-reference";

	RelationalMappingContext context = new JdbcMappingContext(new NamingStrategy() {
		@Override
		public String getReverseColumnName(RelationalPersistentProperty property) {
			return BACK_REFERENCE;
		}
	});

	DataAccessStrategy dataAccessStrategy = mock(DataAccessStrategy.class);
	DefaultJdbcInterpreter interpreter = new DefaultJdbcInterpreter(context, dataAccessStrategy);

	Container container = new Container();
	Element element = new Element();

	InsertRoot<Container> containerInsert = new InsertRoot<>(container);
	Insert<?> insert = new Insert<>(element, PropertyPathUtils.toPath("element", Container.class, context),
			containerInsert);

	private final PersistentPropertyPath<RelationalPersistentProperty> path = context.getPersistentPropertyPath("element",
			Container.class);

	RelationalPersistentProperty elementProperty = path.getRequiredLeafProperty();

	@Test // DATAJDBC-145
	public void insertDoesHonourNamingStrategyForBackReference() {

		container.id = CONTAINER_ID;
		containerInsert.setGeneratedId(CONTAINER_ID);

		interpreter.interpret(insert);

		ArgumentCaptor<EffectiveParentId> argumentCaptor = ArgumentCaptor.forClass(EffectiveParentId.class);
		verify(dataAccessStrategy).insert(eq(element), any(PersistentPropertyPath.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().toParameterMap(path))
				.containsExactly(new SimpleEntry(BACK_REFERENCE, CONTAINER_ID));
	}

	@Test // DATAJDBC-251
	public void idOfParentGetsPassedOnAsAdditionalParameterIfNoIdGotGenerated() {

		container.id = CONTAINER_ID;

		interpreter.interpret(insert);

		ArgumentCaptor<EffectiveParentId> argumentCaptor = ArgumentCaptor.forClass(EffectiveParentId.class);
		verify(dataAccessStrategy).insert(eq(element), any(PersistentPropertyPath.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().toParameterMap(path))
				.containsExactly(new SimpleEntry(BACK_REFERENCE, CONTAINER_ID));
	}

	@Test // DATAJDBC-251
	public void generatedIdOfParentGetsPassedOnAsAdditionalParameter() {

		containerInsert.setGeneratedId(CONTAINER_ID);

		interpreter.interpret(insert);

		ArgumentCaptor<EffectiveParentId> argumentCaptor = ArgumentCaptor.forClass(EffectiveParentId.class);
		verify(dataAccessStrategy).insert(eq(element), any(PersistentPropertyPath.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue().toParameterMap(path))
				.containsExactly(new SimpleEntry(BACK_REFERENCE, CONTAINER_ID));
	}

	static class Container {

		@Id Long id;

		Element element;
	}

	static class Element {}
}
