package org.springframework.data.relational.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link IdOnlyAggregateReference}.
 *
 * @author Myeonghyeon Lee
 */
class IdOnlyAggregateReferenceTest {

	@Test // DATAJDBC-427
	void equals() {

		AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
		AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("1");

		assertThat(reference1).isEqualTo(reference2);
		assertThat(reference2).isEqualTo(reference1);
	}

	@Test // DATAJDBC-427
	void equalsFalse() {

		AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
		AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("2");

		assertThat(reference1).isNotEqualTo(reference2);
		assertThat(reference2).isNotEqualTo(reference1);
	}

	@Test // DATAJDBC-427
	void hashCodeTest() {

		AggregateReference<DummyEntity, String> reference1 = AggregateReference.to("1");
		AggregateReference<DummyEntity, String> reference2 = AggregateReference.to("1");

		assertThat(reference1.hashCode()).isEqualTo(reference2.hashCode());
	}

	private static class DummyEntity {
		private String id;
	}
}
