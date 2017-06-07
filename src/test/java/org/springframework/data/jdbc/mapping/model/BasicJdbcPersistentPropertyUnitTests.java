package org.springframework.data.jdbc.mapping.model;

import static org.assertj.core.api.AssertionsForClassTypes.*;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

import lombok.Data;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.data.jdbc.mapping.context.JdbcMappingContext;
import org.springframework.data.mapping.PropertyHandler;

/**
 * @author Jens Schauder
 */
public class BasicJdbcPersistentPropertyUnitTests {

	@Test // DATAJDBC-104
	public void enumGetsStoredAsString() {
		JdbcPersistentEntity<?> persistentEntity = new JdbcMappingContext().getRequiredPersistentEntity(DummyEntity.class);

		persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) p -> {
			switch (p.getName()) {
				case "someEnum":
					assertThat(p.getColumnType()).isEqualTo(String.class);
					break;
				case "localDateTime":
					assertThat(p.getColumnType()).isEqualTo(Date.class);
					break;
				case "zonedDateTime":
					assertThat(p.getColumnType()).isEqualTo(String.class);
					break;
				default:
					Assertions.fail("property with out assert: " + p.getName());
			}
		});

	}

	@Data
	private static class DummyEntity {
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final ZonedDateTime zonedDateTime;
	}

	private enum SomeEnum {
		@SuppressWarnings("unused")
		ALPHA
	}
}
