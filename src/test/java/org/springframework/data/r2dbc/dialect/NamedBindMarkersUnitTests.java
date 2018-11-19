package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import org.junit.Test;

/**
 * Unit tests for {@link NamedBindMarkers}.
 *
 * @author Mark Paluch
 */
public class NamedBindMarkersUnitTests {

	@Test // gh-15
	public void shouldCreateNewBindMarkers() {

		BindMarkersFactory factory = BindMarkersFactory.named("@", "p", 32);

		BindMarkers bindMarkers1 = factory.create();
		BindMarkers bindMarkers2 = factory.create();

		assertThat(bindMarkers1.next().getPlaceholder()).isEqualTo("@p0");
		assertThat(bindMarkers2.next().getPlaceholder()).isEqualTo("@p0");
	}

	@Test // gh-15
	public void nextShouldIncrementBindMarker() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		BindMarker marker1 = bindMarkers.next();
		BindMarker marker2 = bindMarkers.next();

		assertThat(marker1.getPlaceholder()).isEqualTo("@p0");
		assertThat(marker2.getPlaceholder()).isEqualTo("@p1");
	}

	@Test // gh-15
	public void nextShouldConsiderNameHint() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		BindMarker marker1 = bindMarkers.next("foo.bar?");
		BindMarker marker2 = bindMarkers.next();

		assertThat(marker1.getPlaceholder()).isEqualTo("@p0_foobar");
		assertThat(marker2.getPlaceholder()).isEqualTo("@p1");
	}

	@Test // gh-15
	public void nextShouldConsiderNameLimit() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 10).create();

		BindMarker marker1 = bindMarkers.next("123456789");

		assertThat(marker1.getPlaceholder()).isEqualTo("@p0_1234567");
	}

	@Test // gh-15
	public void bindValueShouldBindByName() {

		Statement<?> statement = mock(Statement.class);

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		bindMarkers.next().bindValue(statement, "foo");
		bindMarkers.next().bindValue(statement, "bar");

		verify(statement).bind("p0", "foo");
		verify(statement).bind("p1", "bar");
	}

	@Test // gh-15
	public void bindNullShouldBindByName() {

		Statement<?> statement = mock(Statement.class);

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		bindMarkers.next(); // ignore

		bindMarkers.next().bindNull(statement, Integer.class);

		verify(statement).bindNull("p1", Integer.class);
	}
}
