package org.springframework.data.r2dbc.repository.query.alias;

import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * 描述: Unit test for {@link org.springframework.data.r2dbc.repository.Query}
 *
 * @author kfyty725
 * @date 2023/12/13 14:55
 * @email kfyty725@hotmail.com
 */
@Configuration
@EnableR2dbcRepositories(considerNestedRepositories = true)
public class StringBasedAliasQueryUnitTests {
    private AnnotationConfigApplicationContext context;
    private UserRepository userRepository;

    @Before
    public void before() {
        context = new AnnotationConfigApplicationContext(StringBasedAliasQueryUnitTests.class);

        userRepository = context.getBean(UserRepository.class);

        init();
    }

    public void init() {
        R2dbcEntityTemplate r2dbcEntityTemplate = context.getBean(R2dbcEntityTemplate.class);
        r2dbcEntityTemplate.getDatabaseClient().sql("create table person (id bigint not null, fans_num int not null,primary key(id));").fetch().one().block();
    }

    @Test
    public void aliasQueryTest() {
        User insert = this.userRepository.save(new User(1L, 1, true)).block();
        User get = this.userRepository.getById(1L).block();
        Assert.assertEquals(insert.getFansNum(), get.getFansNum());         // equals
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        System.setProperty("h2.caseInsensitiveIdentifiers", "true");
        System.setProperty("h2.databaseToUpper", "false");
        System.setProperty("h2.databaseToLower", "false");
        return H2ConnectionFactory.inMemory("test");
    }

    @Bean
    public R2dbcEntityTemplate r2dbcEntityTemplate(ConnectionFactory connectionFactory) {
        return new R2dbcEntityTemplate(connectionFactory);
    }

    @Repository
    private interface UserRepository extends R2dbcRepository<User, Long> {
        @Query("select id, fans_num as fansNum from person where id = :id")
        Mono<User> getById(Long id);
    }

    @Table("person")
    static class User implements Persistable<Long> {
        @Id
        private Long id;
        private Integer fansNum;

        @Transient
        private boolean isNew;

        public User() {
        }

        public User(Long id, Integer fansNum, boolean isNew) {
            this.id = id;
            this.fansNum = fansNum;
            this.isNew = isNew;
        }

        @Override
        public boolean isNew() {
            return isNew;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Integer getFansNum() {
            return fansNum;
        }

        public void setFansNum(Integer fansNum) {
            this.fansNum = fansNum;
        }
    }
}
