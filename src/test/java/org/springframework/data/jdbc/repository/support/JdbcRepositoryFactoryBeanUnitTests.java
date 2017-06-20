package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import javax.sql.DataSource;

import org.assertj.core.api.Condition;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Tests the dependency injection for {@link JdbcRepositoryFactoryBean}.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryFactoryBeanUnitTests {

	static final String JDBC_OPERATIONS_FIELD_NAME = "jdbcOperations";
	static final String EXPECTED_JDBC_OPERATIONS_BEAN_NAME = "jdbcTemplate";
	static final String EXPECTED_NAMED_PARAMETER_JDBC_OPERATIONS_BEAN_NAME = "namedParameterJdbcTemplate";

	ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
	ApplicationContext context = mock(ApplicationContext.class);

	Map<String, DataSource> dataSources = new HashMap<>();
	Map<String, JdbcOperations> jdbcOperations = new HashMap<>();
	Map<String, NamedParameterJdbcOperations> namedJdbcOperations = new HashMap<>();

	{

		when(context.getBeansOfType(DataSource.class)).thenReturn(dataSources);
		when(context.getBeansOfType(JdbcOperations.class)).thenReturn(jdbcOperations);
		when(context.getBeansOfType(NamedParameterJdbcOperations.class)).thenReturn(namedJdbcOperations);
	}

	@Test // DATAJDBC-100
	public void exceptionWithUsefulMessage() {

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(() -> factoryBean.doCreateRepositoryFactory());

	}

	@Test // DATAJDBC-100
	public void singleDataSourceGetsUsedForCreatingRepositoryFactory() {

		DataSource expectedDataSource = mock(DataSource.class);
		dataSources.put("arbitraryName", expectedDataSource);

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedDataSource));
	}

	@Test // DATAJDBC-100
	public void multipleDataSourcesGetDisambiguatedByName() {

		DataSource expectedDataSource = mock(DataSource.class);
		dataSources.put("dataSource", expectedDataSource);
		dataSources.put("arbitraryName", mock(DataSource.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedDataSource));
	}

	@Test // DATAJDBC-100
	public void singleJdbcOperationsUsedForCreatingRepositoryFactory() {

		JdbcOperations expectedOperations = mock(JdbcOperations.class);
		jdbcOperations.put("arbitraryName", expectedOperations);

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void multipleJdbcOperationsGetDisambiguatedByName() {

		JdbcOperations expectedOperations = mock(JdbcOperations.class);
		jdbcOperations.put(EXPECTED_JDBC_OPERATIONS_BEAN_NAME, expectedOperations);
		jdbcOperations.put("arbitraryName", mock(JdbcOperations.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void singleNamedJdbcOperationsUsedForCreatingRepositoryFactory() {

		NamedParameterJdbcOperations expectedOperations = mock(NamedParameterJdbcOperations.class);
		namedJdbcOperations.put("arbitraryName", expectedOperations);

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void multipleNamedJdbcOperationsGetDisambiguatedByName() {

		NamedParameterJdbcOperations expectedOperations = mock(NamedParameterJdbcOperations.class);
		namedJdbcOperations.put(EXPECTED_NAMED_PARAMETER_JDBC_OPERATIONS_BEAN_NAME, expectedOperations);
		namedJdbcOperations.put("arbitraryName", mock(NamedParameterJdbcOperations.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void namedParameterJdbcOperationsTakePrecedenceOverDataSource() {

		NamedParameterJdbcOperations expectedOperations = mock(NamedParameterJdbcOperations.class);
		namedJdbcOperations.put("arbitraryName", expectedOperations);
		dataSources.put("arbitraryName", mock(DataSource.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void jdbcOperationsTakePrecedenceOverDataSource() {

		JdbcOperations expectedOperations = mock(JdbcOperations.class);
		jdbcOperations.put("arbitraryName", expectedOperations);
		dataSources.put("arbitraryName", mock(DataSource.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	@Test // DATAJDBC-100
	public void namedParameterJdbcOperationsTakePrecedenceOverJdbcOperations() {

		NamedParameterJdbcOperations expectedOperations = mock(NamedParameterJdbcOperations.class);
		namedJdbcOperations.put("arbitraryName", expectedOperations);
		jdbcOperations.put("arbitraryName", mock(JdbcOperations.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThat(factoryBean.doCreateRepositoryFactory()).is(using(expectedOperations));
	}

	private Condition<? super RepositoryFactorySupport> using(NamedParameterJdbcOperations expectedOperations) {

		Predicate<RepositoryFactorySupport> predicate = r -> getField(r, JDBC_OPERATIONS_FIELD_NAME) == expectedOperations;
		return new Condition<>(predicate, "uses " + expectedOperations);
	}

	private Condition<? super RepositoryFactorySupport> using(JdbcOperations expectedOperations) {

		Predicate<RepositoryFactorySupport> predicate = r -> {
			NamedParameterJdbcOperations namedOperations = (NamedParameterJdbcOperations) getField(r,
					JDBC_OPERATIONS_FIELD_NAME);
			return namedOperations.getJdbcOperations() == expectedOperations;
		};

		return new Condition<>(predicate, "uses " + expectedOperations);
	}

	private Condition<? super RepositoryFactorySupport> using(DataSource expectedDataSource) {

		Predicate<RepositoryFactorySupport> predicate = r -> {

			NamedParameterJdbcOperations namedOperations = (NamedParameterJdbcOperations) getField(r,
					JDBC_OPERATIONS_FIELD_NAME);
			JdbcTemplate jdbcOperations = (JdbcTemplate) namedOperations.getJdbcOperations();
			return jdbcOperations.getDataSource() == expectedDataSource;
		};

		return new Condition<>(predicate, "using " + expectedDataSource);
	}

	private static class DummyEntity {
		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
