/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.r2dbc.aot;

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.event.AfterConvertCallback;
import org.springframework.data.r2dbc.mapping.event.AfterSaveCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.r2dbc.repository.support.SimpleR2dbcRepository;

/**
 * {@link RuntimeHintsRegistrar} for R2DBC.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
class R2dbcRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, ClassLoader classLoader) {

		hints.reflection()
				.registerTypes(
						Arrays.asList(TypeReference.of(SimpleR2dbcRepository.class), TypeReference.of(AfterConvertCallback.class),
								TypeReference
										.of(BeforeConvertCallback.class),
								TypeReference.of(BeforeSaveCallback.class), TypeReference.of(AfterSaveCallback.class)),
				hint -> hint.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_PUBLIC_METHODS));

		for (Class<?> simpleType : PostgresDialect.INSTANCE.simpleTypes()) {
			hints.reflection().registerType(TypeReference.of(simpleType), MemberCategory.PUBLIC_CLASSES);
		}
	}
}
