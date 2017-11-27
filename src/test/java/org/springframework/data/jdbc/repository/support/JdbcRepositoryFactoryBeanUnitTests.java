package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Tests the dependency injection for {@link JdbcRepositoryFactoryBean}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcRepositoryFactoryBeanUnitTests {

	JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean;

	StubRepositoryFactorySupport factory;
	@Mock ListableBeanFactory beanFactory;
	@Mock Repository<?, ?> repository;
	@Mock DataAccessStrategy dataAccessStrategy;
	@Mock JdbcMappingContext mappingContext;

	@Before
	public void setUp() {

		factory = Mockito.spy(new StubRepositoryFactorySupport(repository));

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

	/**
	 * required to trick Mockito on invoking protected getRepository(Class<T> repositoryInterface, Optional<Object>
	 * customImplementation
	 */
	private static class StubRepositoryFactorySupport extends RepositoryFactorySupport {

		private final Repository<?, ?> repository;

		private StubRepositoryFactorySupport(Repository<?, ?> repository) {
			this.repository = repository;
		}

		@Override
		public <T> T getRepository(Class<T> repositoryInterface, RepositoryComposition.RepositoryFragments fragments) {
			return (T) repository;
		}

		@Override
		public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
			return null;
		}

		@Override
		protected Object getTargetRepository(RepositoryInformation metadata) {
			return null;
		}

		@Override
		protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
			return null;
		}
	}
	
	private static class DummyEntity {
		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
