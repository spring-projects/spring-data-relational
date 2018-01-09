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
package org.springframework.data.jdbc.core.conversion;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import org.junit.Test;

/**
 * Unit tests for {@link DbAction}s
 *
 * @author Jens Schauder
 */
public class DbActionUnitTests {

	@Test // DATAJDBC-150
	public void exceptionFromActionContainsUsefulInformationWhenInterpreterFails() {

		DummyEntity entity = new DummyEntity();
		DbAction.Insert<DummyEntity> insert = DbAction.insert(entity, JdbcPropertyPath.from("someName", DummyEntity.class),
				null);

		Interpreter failingInterpreter = mock(Interpreter.class);
		doThrow(new RuntimeException()).when(failingInterpreter).interpret(any(DbAction.Insert.class));

		assertThatExceptionOfType(DbActionExecutionException.class) //
                .isThrownBy(() -> insert.executeWith(failingInterpreter)) //
				.withMessageContaining("Insert") //
                .withMessageContaining(entity.toString());

	}

	static class DummyEntity {
		String someName;
	}
}
