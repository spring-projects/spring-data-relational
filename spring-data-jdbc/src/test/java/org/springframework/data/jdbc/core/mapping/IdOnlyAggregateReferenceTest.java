package org.springframework.data.jdbc.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.jdbc.core.mapping.AggregateReference.IdOnlyAggregateReference;

/**
 * Unit tests for the {@link IdOnlyAggregateReference}.
 *
 * @author Myeonghyeon Lee
 */
public class IdOnlyAggregateReferenceTest {
    @Test   // DATAJDBC-427
    public void equals() {
        AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
        AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("1");
        assertThat(reference1.equals(reference2)).isTrue();
        assertThat(reference2.equals(reference1)).isTrue();
    }

    @Test   // DATAJDBC-427
    public void equalsFalse() {
        AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
        AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("2");
        assertThat(reference1.equals(reference2)).isFalse();
        assertThat(reference2.equals(reference1)).isFalse();
    }

    @Test   // DATAJDBC-427
    public void hashCodeTest() {
        AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
        AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("1");
        assertThat(reference1.hashCode()).isEqualTo(reference2.hashCode());
    }

    private static class DummyEntity {
        private String id;
    }
}
