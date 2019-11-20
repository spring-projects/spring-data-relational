/*
 * Copyright 2017-2019 the original author or authors.
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

import java.io.IOException;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;


/**
 * Infrastructure configuration for integration tests.
 *
 * @author Moises Cisneros
 */
@Configuration
@ComponentScan // To pick up configuration classes (per activated profile)
@Import(TestConfiguration.class)
public class QueryNamedTestConfiguration {

	
	@Autowired JdbcRepositoryFactory factory;

	@PostConstruct()
	public void factory() throws IOException {
		PropertiesFactoryBean properties = new PropertiesFactoryBean();
		properties.setLocation(new ClassPathResource("META-INF/jdbc-named-queries.properties"));
		NamedQueries namedQueries = null;
			properties.afterPropertiesSet();
			namedQueries = new PropertiesBasedNamedQueries(properties.getObject());
		factory.setNamedQueries(namedQueries);
	}		
}

