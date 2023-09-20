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

package org.springframework.data.jdbc.testing;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.test.context.ActiveProfilesResolver;
import org.springframework.test.context.support.DefaultActiveProfilesResolver;

/**
 * A {@link ActiveProfilesResolver} combining the profile configurations from environement, system properties and
 * {@link org.springframework.test.context.ActiveProfiles} annotations.
 *
 * @author Jens Schauder
 */
class CombiningActiveProfileResolver implements ActiveProfilesResolver {

	private static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";
	private final DefaultActiveProfilesResolver defaultActiveProfilesResolver = new DefaultActiveProfilesResolver();

	@Override
	public String[] resolve(Class<?> testClass) {

		Set<String> combinedProfiles = new LinkedHashSet<>();

		combinedProfiles.addAll(Arrays.asList(defaultActiveProfilesResolver.resolve(testClass)));
		combinedProfiles.addAll(Arrays.asList(getSystemProfiles()));
		combinedProfiles.addAll(Arrays.asList(getEnvironmentProfiles()));

		return combinedProfiles.toArray(new String[0]);
	}

	private static String[] getSystemProfiles() {

		if (System.getProperties().containsKey(SPRING_PROFILES_ACTIVE)) {

			String profiles = System.getProperty(SPRING_PROFILES_ACTIVE);
			return profiles.split("\\s*,\\s*");
		}

		return new String[0];
	}

	private String[] getEnvironmentProfiles() {

		if (System.getenv().containsKey(SPRING_PROFILES_ACTIVE)) {

			String profiles = System.getenv().get(SPRING_PROFILES_ACTIVE);
			return profiles.split("\\s*,\\s*");
		}

		return new String[0];

	}

}
