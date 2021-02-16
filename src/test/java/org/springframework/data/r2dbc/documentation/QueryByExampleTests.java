package org.springframework.data.r2dbc.documentation;

import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.domain.ExampleMatcher.GenericPropertyMatchers.*;

import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.r2dbc.repository.R2dbcRepository;

public class QueryByExampleTests {

	private EmployeeRepository repository;

	@Test
	void queryByExampleSimple() {

		// tag::example[]
		Employee employee = new Employee(); // <1>
		employee.setName("Frodo");

		Example<Employee> example = Example.of(employee); // <2>

		Flux<Employee> employees = repository.findAll(example); // <3>

		// do whatever with the flux
		// end::example[]
	}

	@Test
	void queryByExampleCustomMatcher() {

		// tag::example-2[]
		Employee employee = new Employee();
		employee.setName("Baggins");
		employee.setRole("ring bearer");

		ExampleMatcher matcher = matching() // <1>
				.withMatcher("name", endsWith()) // <2>
				.withIncludeNullValues() // <3>
				.withIgnorePaths("role"); // <4>
		Example<Employee> example = Example.of(employee, matcher); // <5>

		Flux<Employee> employees = repository.findAll(example);

		// do whatever with the flux
		// end::example-2[]
	}

	@Data
	@NoArgsConstructor
	public class Employee {

		private @Id Integer id;
		private String name;
		private String role;
	}

	public interface EmployeeRepository extends R2dbcRepository<Employee, Integer> {}
}
