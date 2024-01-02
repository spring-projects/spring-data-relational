/*
 * Copyright 2018-2024 the original author or authors.
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

import org.springframework.lang.Nullable;

/**
 * Utility class for analyzing DbActions in tests.
 *
 * @author Jens Schauder
 * @author Chirag Tailor
 */
final class DbActionTestSupport {

	private DbActionTestSupport() {
		throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
	}

	static String extractPath(DbAction<?> action) {

		if (action instanceof DbAction.WithPropertyPath) {
			return ((DbAction.WithPropertyPath<?>) action).getPropertyPath().toDotPath();
		}

		return "";
	}

	static boolean isWithDependsOn(DbAction<?> dbAction) {
		return dbAction instanceof DbAction.WithDependingOn;
	}

	@Nullable
	static Class<?> actualEntityType(DbAction<?> a) {

		if (a instanceof DbAction.WithEntity) {
			return ((DbAction.WithEntity<?>) a).getEntity().getClass();
		}
		return null;
	}

	@Nullable
	static IdValueSource insertIdValueSource(DbAction<?> action) {

		if (action instanceof DbAction.WithEntity<?>) {
			return ((DbAction.WithEntity<?>) action).getIdValueSource();
		} else if (action instanceof DbAction.BatchInsert) {
			return ((DbAction.BatchInsert<?>) action).getBatchValue();
		} else if (action instanceof DbAction.BatchInsertRoot<?>) {
			return ((DbAction.BatchInsertRoot<?>) action).getBatchValue();
		} else {
			return null;
		}
	}
}
