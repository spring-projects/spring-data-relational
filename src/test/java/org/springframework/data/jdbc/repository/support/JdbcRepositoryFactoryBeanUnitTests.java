package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.assertj.core.api.Condition;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategy;
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

	static final String EXPECTED_JDBC_OPERATIONS_BEAN_NAME = "jdbcTemplate";
	static final String EXPECTED_NAMED_PARAMETER_JDBC_OPERATIONS_BEAN_NAME = "namedParameterJdbcTemplate";

	static final String ACCESS_STRATEGY_FIELD_NAME_IN_FACTORY = "accessStrategy";
	static final String OPERATIONS_FIELD_NAME_IN_DEFAULT_ACCESS_STRATEGY = "operations";

	ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
	ApplicationContext context = mock(ApplicationContext.class);

	Map<String, DataSource> dataSources = new HashMap<>();
	Map<String, JdbcOperations> jdbcOperations = new HashMap<>();
	Map<String, NamedParameterJdbcOperations> namedJdbcOperations = new HashMap<>();
	Map<String, SqlSessionFactory> sqlSessionFactories = new HashMap<>();

	{

		when(context.getBeansOfType(DataSource.class)).thenReturn(dataSources);
		when(context.getBeansOfType(JdbcOperations.class)).thenReturn(jdbcOperations);
		when(context.getBeansOfType(NamedParameterJdbcOperations.class)).thenReturn(namedJdbcOperations);
		when(context.getBeansOfType(SqlSessionFactory.class)).thenReturn(sqlSessionFactories);
	}

	@Test // DATAJDBC-100
	public void exceptionWithUsefulMessage() {

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		assertThatExceptionOfType(IllegalStateException.class) //
				.isThrownBy(factoryBean::doCreateRepositoryFactory);

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

	@Test // DATAJDBC-123
	public void withoutSqlSessionFactoryThereIsNoMyBatisIntegration() {

		dataSources.put("anyname", mock(DataSource.class));
		sqlSessionFactories.clear();

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		RepositoryFactorySupport factory = factoryBean.doCreateRepositoryFactory();

		assertThat(findDataAccessStrategy(factory, MyBatisDataAccessStrategy.class)).isNull();
	}

	@Test // DATAJDBC-123
	public void withSqlSessionFactoryThereIsMyBatisIntegration() {

		dataSources.put("anyname", mock(DataSource.class));
		sqlSessionFactories.put("anyname", mock(SqlSessionFactory.class));

		JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean = //
				new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class, eventPublisher, context);

		RepositoryFactorySupport factory = factoryBean.doCreateRepositoryFactory();

		assertThat(findDataAccessStrategy(factory, MyBatisDataAccessStrategy.class)).isNotNull();
	}

	private Condition<? super RepositoryFactorySupport> using(NamedParameterJdbcOperations expectedOperations) {

		Predicate<RepositoryFactorySupport> predicate = r -> extractNamedParameterJdbcOperations(r) == expectedOperations;
		return new Condition<>(predicate, "uses " + expectedOperations);
	}

	private NamedParameterJdbcOperations extractNamedParameterJdbcOperations(RepositoryFactorySupport r) {

		DefaultDataAccessStrategy defaultDataAccessStrategy = findDataAccessStrategy(r, DefaultDataAccessStrategy.class);
		return (NamedParameterJdbcOperations) getField(defaultDataAccessStrategy,
				OPERATIONS_FIELD_NAME_IN_DEFAULT_ACCESS_STRATEGY);
	}

	private Condition<? super RepositoryFactorySupport> using(JdbcOperations expectedOperations) {

		Predicate<RepositoryFactorySupport> predicate = r -> extractNamedParameterJdbcOperations(r)
				.getJdbcOperations() == expectedOperations;

		return new Condition<>(predicate, "uses " + expectedOperations);
	}

	private Condition<? super RepositoryFactorySupport> using(DataSource expectedDataSource) {

		Predicate<RepositoryFactorySupport> predicate = r -> {

			NamedParameterJdbcOperations namedOperations = extractNamedParameterJdbcOperations(r);
			JdbcTemplate jdbcOperations = (JdbcTemplate) namedOperations.getJdbcOperations();
			return jdbcOperations.getDataSource() == expectedDataSource;
		};

		return new Condition<>(predicate, "using " + expectedDataSource);
	}

	private static <T extends DataAccessStrategy> T findDataAccessStrategy(RepositoryFactorySupport r, Class<T> type) {

		DataAccessStrategy accessStrategy = (DataAccessStrategy) getField(r, ACCESS_STRATEGY_FIELD_NAME_IN_FACTORY);
		return findDataAccessStrategy(accessStrategy, type);
	}

	private static <T extends DataAccessStrategy> T findDataAccessStrategy(DataAccessStrategy accessStrategy,
			Class<T> type) {

		if (type.isInstance(accessStrategy))
			return (T) accessStrategy;

		if (accessStrategy instanceof DelegatingDataAccessStrategy) {
			return findDataAccessStrategy((DataAccessStrategy) getField(accessStrategy, "delegate"), type);
		}

		if (accessStrategy instanceof CascadingDataAccessStrategy) {
			List<DataAccessStrategy> strategies = (List<DataAccessStrategy>) getField(accessStrategy, "strategies");
			return strategies.stream() //
					.map((DataAccessStrategy das) -> findDataAccessStrategy(das, type)) //
					.filter(Objects::nonNull) //
					.findFirst() //
					.orElse(null);
		}

		return null;
	}

	private static class DummyEntity {
		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
