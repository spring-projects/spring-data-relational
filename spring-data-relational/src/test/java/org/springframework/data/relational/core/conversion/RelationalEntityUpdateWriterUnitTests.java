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
package org.springframework.data.relational.core.conversion;

import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.conversion.AggregateChange.Kind;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the {@link RelationalEntityUpdateWriter}
 *
 * @author Jens Schauder
 * @author Thomas Lang
 */
@RunWith(MockitoJUnitRunner.class)
public class RelationalEntityUpdateWriterUnitTests {

    public static final long SOME_ENTITY_ID = 23L;
    RelationalEntityUpdateWriter converter = new RelationalEntityUpdateWriter(new RelationalMappingContext());

    @Test // DATAJDBC-112
    public void existingEntityGetsConvertedToDeletePlusUpdate() {

        SingleReferenceEntity entity = new SingleReferenceEntity(SOME_ENTITY_ID);

        AggregateChange<RelationalEntityWriterUnitTests.SingleReferenceEntity> aggregateChange = //
                new AggregateChange(Kind.SAVE, SingleReferenceEntity.class, entity);

        converter.write(entity, aggregateChange);

        assertThat(aggregateChange.getActions()) //
                .extracting(DbAction::getClass, DbAction::getEntityType, this::extractPath, this::actualEntityType,
                        this::isWithDependsOn) //
                .containsExactly( //
                        tuple(DbAction.Delete.class, Element.class, "other", null, false), //
                        tuple(DbAction.UpdateRoot.class, SingleReferenceEntity.class, "", SingleReferenceEntity.class, false) //
                );
    }

    private String extractPath(DbAction action) {

        if (action instanceof DbAction.WithPropertyPath) {
            return ((DbAction.WithPropertyPath<?>) action).getPropertyPath().toDotPath();
        }

        return "";
    }

    private boolean isWithDependsOn(DbAction dbAction) {
        return dbAction instanceof DbAction.WithDependingOn;
    }

    private Class<?> actualEntityType(DbAction a) {

        if (a instanceof DbAction.WithEntity) {
            return ((DbAction.WithEntity) a).getEntity().getClass();
        }
        return null;
    }

    @RequiredArgsConstructor
    static class SingleReferenceEntity {

        @Id
        final Long id;
        Element other;
        // should not trigger own Dbaction
        String name;
    }

    @RequiredArgsConstructor
    private static class Element {
        @Id
        final Long id;
    }

}
