/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.relational.degraph;

import static de.schauderhaft.degraph.check.JCheck.*;
import static org.hamcrest.MatcherAssert.*;

import de.schauderhaft.degraph.check.JCheck;
import org.junit.jupiter.api.Test;
import scala.runtime.AbstractFunction1;

/**
 * Test package dependencies for violations.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class DependencyTests {

	@Test // DATAJDBC-114
	public void cycleFree() {

		assertThat( //
				classpath() //
						.noJars() //
						.including("org.springframework.data.relational.**") //
						.filterClasspath("*target/classes") // exclude test code
						.printOnFailure("degraph-relational.graphml"),
				JCheck.violationFree());
	}

	@Test // DATAJDBC-220
	public void acrossModules() {

		assertThat( //
				classpath() //
						// include only Spring Data related classes (for example no JDK code)
						.including("org.springframework.data.**") //
						.excluding("org.springframework.data.relational.core.sql.**") //
						.excluding("org.springframework.data.repository.query.parser.**") //
						.filterClasspath(new AbstractFunction1<String, Object>() {
							@Override
							public Object apply(String s) { //
								// only the current module + commons
								return s.endsWith("target/classes") || s.contains("spring-data-commons");
							}
						}) // exclude test code
						.withSlicing("sub-modules", // sub-modules are defined by any of the following pattern.
								"org.springframework.data.relational.(**).*", //
								"org.springframework.data.(**).*") //
						.printTo("degraph-across-modules.graphml"), // writes a graphml to this location
				JCheck.violationFree());
	}
}
