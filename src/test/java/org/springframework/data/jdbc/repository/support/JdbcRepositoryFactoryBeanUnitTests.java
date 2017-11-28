package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;

/**
 * Tests the dependency injection for {@link JdbcRepositoryFactoryBean}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcRepositoryFactoryBeanUnitTests {

	JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean;

	@Mock ListableBeanFactory beanFactory;
	@Mock Repository<?, ?> repository;
	@Mock DataAccessStrategy dataAccessStrategy;
	@Mock JdbcMappingContext mappingContext;

	@Before
	public void setUp() {

		// Setup standard configuration
		factoryBean = new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class);
	}

	@Test
	public void setsUpBasicInstanceCorrectly() {

		factoryBean.setDataAccessStrategy(dataAccessStrategy);
		factoryBean.setMappingContext(mappingContext);
		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getObject()).isNotNull();
	}

	@Test(expected = IllegalArgumentException.class)
	public void requiresListableBeanFactory() {

		factoryBean.setBeanFactory(mock(BeanFactory.class));
	}

	private static class DummyEntity {
		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
