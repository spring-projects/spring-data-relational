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
package org.springframework.data.jdbc.degraph;
import static de.schauderhaft.degraph.check.JCheck.classpath;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import de.schauderhaft.degraph.check.JCheck;

/**
 * @author Jens Schauder
 */
public class DependencyTests {

	@Test public void test() {
		assertThat(	classpath()
				.noJars()
				.including("org.springframework.data.jdbc.**")
				.filterClasspath("*target/classes")
				.printOnFailure("degraph.graphml"), JCheck.violationFree());

	}

}
