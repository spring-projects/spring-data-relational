/*
 * Copyright 2020-2024 the original author or authors.
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

import static org.springframework.data.jdbc.testing.MsSqlDataSourceConfiguration.*;

import org.junit.AssumptionViolatedException;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListener;
import org.testcontainers.utility.LicenseAcceptance;

/**
 * {@link TestExecutionListener} to selectively skip tests if the license for a particular database container was not
 * accepted.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@Order(Integer.MIN_VALUE)
class LicenseListener implements TestExecutionListener {

	private final StandardEnvironment environment = new StandardEnvironment();

	@Override
	public void prepareTestInstance(TestContext testContext) {

		if (environment.matchesProfiles(DatabaseType.DB2.getProfile())) {
			assumeLicenseAccepted(Db2DataSourceConfiguration.DOCKER_IMAGE_NAME);
		}

		if (environment.matchesProfiles(DatabaseType.SQL_SERVER.getProfile())) {
			assumeLicenseAccepted(MS_SQL_SERVER_VERSION);
		}
	}

	private void assumeLicenseAccepted(String imageName) {

		try {
			LicenseAcceptance.assertLicenseAccepted(imageName);
		} catch (IllegalStateException e) {

			if (environment.getProperty("on-missing-license", "fail").equals("ignore-test")) {
				throw new AssumptionViolatedException(e.getMessage(), e);
			}

			throw new IllegalStateException(
					"You need to accept the license for the database with which you are testing or set \"ignore-missing-license\" as active profile in order to skip tests for which a license is missing.",
					e);
		}
	}

}
