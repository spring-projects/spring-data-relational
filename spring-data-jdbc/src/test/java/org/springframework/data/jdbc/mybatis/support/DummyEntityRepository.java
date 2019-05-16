package org.springframework.data.jdbc.mybatis.support;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jdbc.mybatis.DummyEntity;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

    @MybatisQuery
    DummyEntity notSpringDataNamingConventionsFindById(@Param("id") Long id);

    @MybatisQuery
    Optional<DummyEntity> notSpringDataNamingConventionsFindByIdOptional(@Param("id") Long id);

    @Modifying
    @MybatisQuery
    boolean notSpringDataNamingConventionsUpdateById(@Param("id") Long id, @Param("newName") String newName);

    @Modifying
    @MybatisQuery
    int notSpringDataNamingConventionsUpdateByIdReturnUpdatedRowCount(@Param("id") Long id, @Param("newName") String newName);
}