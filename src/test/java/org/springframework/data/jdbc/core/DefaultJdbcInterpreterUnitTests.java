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
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.conversion.DbAction;
import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.jdbc.core.mapping.NamingStrategy;
import org.springframework.data.jdbc.core.conversion.JdbcPropertyPath;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link DefaultJdbcInterpreter}
 *
 * @author Jens Schauder
 */
public class DefaultJdbcInterpreterUnitTests {

	static final long CONTAINER_ID = 23L;
	static final String BACK_REFERENCE = "back-reference";

	JdbcMappingContext context = new JdbcMappingContext(new NamingStrategy() {
		@Override
		public String getReverseColumnName(JdbcPersistentProperty property) {
			return BACK_REFERENCE;
		}
	});

	DataAccessStrategy dataAccessStrategy = mock(DataAccessStrategy.class);
	DefaultJdbcInterpreter interpreter = new DefaultJdbcInterpreter(context, dataAccessStrategy);

	@Test // DATAJDBC-145
	public void insertDoesHonourNamingStrategyForBackReference() {

		Container container = new Container();
		container.id = CONTAINER_ID;

		Element element = new Element();

		Insert<?> containerInsert = DbAction.insert(container, JdbcPropertyPath.from("", Container.class), null);
		Insert<?> insert = DbAction.insert(element, JdbcPropertyPath.from("element", Container.class), containerInsert);

		interpreter.interpret(insert);

		ArgumentCaptor<Map<String, Object>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
		verify(dataAccessStrategy).insert(eq(element), eq(Element.class), argumentCaptor.capture());

		assertThat(argumentCaptor.getValue()).containsExactly(new SimpleEntry(BACK_REFERENCE, CONTAINER_ID));
	}

	static class Container {

		@Id Long id;

		Element element;
	}

	static class Element {}
}
