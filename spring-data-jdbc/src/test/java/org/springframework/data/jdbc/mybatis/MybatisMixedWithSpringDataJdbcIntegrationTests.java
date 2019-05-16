package org.springframework.data.jdbc.mybatis;

import junit.framework.AssertionFailedError;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.jdbc.mybatis.support.DummyEntityRepository;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Songling.Dong
 * Created on 2019/5/15.
 * Tests the integration for Mybatis mixed with spring-data-jdbc
 */
@ContextConfiguration
@ActiveProfiles("hsql")
@Transactional
public class MybatisMixedWithSpringDataJdbcIntegrationTests {


    @ClassRule
    public static final SpringClassRule classRule = new SpringClassRule();
    @Rule
    public SpringMethodRule methodRule = new SpringMethodRule();

    @Autowired
    SqlSessionFactory sqlSessionFactory;
    @Autowired
    DummyEntityRepository repository;

    @Test // DATAJDBC-178
    public void myBatisGetsUsedForInsertAndSelect() {

        DummyEntity entity = new DummyEntity(null, "some name");
        DummyEntity saved = repository.save(entity);

        assertThat(saved.id).isNotNull();

        DummyEntity reloaded = repository.findById(saved.id).orElseThrow(AssertionFailedError::new);

        DummyEntity reloadedOptionalByMybatis = repository.notSpringDataNamingConventionsFindByIdOptional(saved.id).orElseThrow(AssertionFailedError::new);

        assertThat(reloaded.name).isEqualTo(reloadedOptionalByMybatis.name);
        assertThat(reloaded.id).isEqualTo(reloadedOptionalByMybatis.id);


        String newName = "John doe";
        boolean updateSuccess = repository.notSpringDataNamingConventionsUpdateById(saved.id, newName);
        assertThat(updateSuccess).isTrue();
        DummyEntity dummyEntityWithNewName = repository.findById(saved.id).orElseThrow(AssertionFailedError::new);
        assertThat(dummyEntityWithNewName.name).isEqualTo(newName);

        String newName2 = "John doe DSL";
        int updateRowCount = repository.notSpringDataNamingConventionsUpdateByIdReturnUpdatedRowCount(saved.id, newName2);
        assertThat(updateRowCount).isEqualTo(1);
        DummyEntity dummyEntityWithNewName2 = repository.findById(saved.id).orElseThrow(AssertionFailedError::new);
        assertThat(dummyEntityWithNewName2.name).isEqualTo(newName2);
    }

    @org.springframework.context.annotation.Configuration
    @Import(TestConfiguration.class)
    @EnableJdbcRepositories
    static class Config {

        @Bean
        Class<?> testClass() {
            return MybatisMixedWithSpringDataJdbcIntegrationTests.class;
        }

        @Bean
        SqlSessionFactoryBean createSessionFactory(EmbeddedDatabase db) throws IOException {

            Configuration configuration = new Configuration();
            configuration.getTypeAliasRegistry().registerAlias("MyBatisContext", MyBatisContext.class);
            configuration.getTypeAliasRegistry().registerAlias(DummyEntity.class);

            configuration.setMapUnderscoreToCamelCase(true);

            SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
            sqlSessionFactoryBean.setDataSource(db);
            sqlSessionFactoryBean.setConfiguration(configuration);
            sqlSessionFactoryBean.setMapperLocations(new PathMatchingResourcePatternResolver()
                    .getResources("classpath*:org/springframework/data/jdbc/mybatis/support/*.xml"));

            return sqlSessionFactoryBean;
        }

        @Bean
        SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory factory) {
            return new SqlSessionTemplate(factory);
        }


    }


}

