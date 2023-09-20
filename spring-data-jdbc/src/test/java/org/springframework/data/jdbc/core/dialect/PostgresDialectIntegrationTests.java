package org.springframework.data.jdbc.core.dialect;

import static org.assertj.core.api.Assertions.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;

/**
 * Integration tests for PostgreSQL Dialect. Start this test with {@code -Dspring.profiles.active=postgres}.
 *
 * @author Nikita Konev
 * @author Mark Paluch
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.POSTGRES)
public class PostgresDialectIntegrationTests {

	@Autowired CustomerRepository customerRepository;

	@Test // GH-920
	void shouldSaveAndLoadJson() throws SQLException {

		PGobject sessionData = new PGobject();
		sessionData.setType("jsonb");
		sessionData.setValue("{\"hello\": \"json\"}");

		Customer saved = customerRepository
				.save(new Customer(null, "Adam Smith", new JsonHolder("{\"hello\": \"world\"}"), sessionData));

		Optional<Customer> loaded = customerRepository.findById(saved.getId());

		assertThat(loaded).hasValueSatisfying(actual -> {

			assertThat(actual.getPersonData().getContent()).isEqualTo("{\"hello\": \"world\"}");
			assertThat(actual.getSessionData().getValue()).isEqualTo("{\"hello\": \"json\"}");
		});
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = CustomerRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

		@Bean
		CustomConversions jdbcCustomConversions(Dialect dialect) {
			SimpleTypeHolder simpleTypeHolder = new SimpleTypeHolder(dialect.simpleTypes(), JdbcSimpleTypes.HOLDER);

			return new JdbcCustomConversions(
					CustomConversions.StoreConversions.of(simpleTypeHolder, storeConverters(dialect)), userConverters());
		}

		private List<Object> storeConverters(Dialect dialect) {

			List<Object> converters = new ArrayList<>();
			converters.addAll(dialect.getConverters());
			converters.addAll(JdbcCustomConversions.storeConverters());
			return converters;
		}

		private List<Object> userConverters() {
			return Arrays.asList(JsonHolderToPGobjectConverter.INSTANCE, PGobjectToJsonHolderConverter.INSTANCE);
		}
	}

	enum JsonHolderToPGobjectConverter implements Converter<JsonHolder, PGobject> {

		INSTANCE;

		@Override
		public PGobject convert(JsonHolder source) {
			PGobject result = new PGobject();
			result.setType("json");
			try {
				result.setValue(source.getContent());
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			return result;
		}
	}

	enum PGobjectToJsonHolderConverter implements Converter<PGobject, JsonHolder> {

		INSTANCE;

		@Override
		public JsonHolder convert(PGobject source) {
			return new JsonHolder(source.getValue());
		}
	}

	@Table("customers")
	public static final class Customer {

		@Id private final Long id;
		private final String name;
		private final JsonHolder personData;
		private final PGobject sessionData;

		public Customer(Long id, String name, JsonHolder personData, PGobject sessionData) {
			this.id = id;
			this.name = name;
			this.personData = personData;
			this.sessionData = sessionData;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public JsonHolder getPersonData() {
			return this.personData;
		}

		public PGobject getSessionData() {
			return this.sessionData;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof final Customer other))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$personData = this.getPersonData();
			final Object other$personData = other.getPersonData();
			if (!Objects.equals(this$personData, other$personData))
				return false;
			final Object this$sessionData = this.getSessionData();
			final Object other$sessionData = other.getSessionData();
			return Objects.equals(this$sessionData, other$sessionData);
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $personData = this.getPersonData();
			result = result * PRIME + ($personData == null ? 43 : $personData.hashCode());
			final Object $sessionData = this.getSessionData();
			result = result * PRIME + ($sessionData == null ? 43 : $sessionData.hashCode());
			return result;
		}

		public String toString() {
			return "PostgresDialectIntegrationTests.Customer(id=" + this.getId() + ", name=" + this.getName()
					+ ", personData=" + this.getPersonData() + ", sessionData=" + this.getSessionData() + ")";
		}
	}

	public static class JsonHolder {
		String content;

		public JsonHolder(String content) {
			this.content = content;
		}

		public JsonHolder() {}

		public String getContent() {
			return this.content;
		}

		public void setContent(String content) {
			this.content = content;
		}
	}

	interface CustomerRepository extends CrudRepository<Customer, Long> {}

}
