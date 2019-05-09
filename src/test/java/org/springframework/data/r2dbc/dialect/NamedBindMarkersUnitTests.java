package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

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

		String[] prefixes = { "$", "?" };

		for (String prefix : prefixes) {

			BindMarkers bindMarkers = BindMarkersFactory.named(prefix, "p", 32).create();

			BindMarker marker1 = bindMarkers.next();
			BindMarker marker2 = bindMarkers.next();

			assertThat(marker1.getPlaceholder()).isEqualTo(prefix + "p0");
			assertThat(marker2.getPlaceholder()).isEqualTo(prefix + "p1");
		}
	}

	@Test // gh-15
	public void nextShouldConsiderNameHint() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "x", 32).create();

		BindMarker marker1 = bindMarkers.next("foo1bar");
		BindMarker marker2 = bindMarkers.next();

		assertThat(marker1.getPlaceholder()).isEqualTo("@x0foo1bar");
		assertThat(marker2.getPlaceholder()).isEqualTo("@x1");
	}

	@Test // gh-15
	public void nextShouldConsiderFilteredNameHint() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32, s -> {

			return s.chars().filter(Character::isAlphabetic)
					.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

		}).create();

		BindMarker marker1 = bindMarkers.next("foo1.bar?");
		BindMarker marker2 = bindMarkers.next();

		assertThat(marker1.getPlaceholder()).isEqualTo("@p0foobar");
		assertThat(marker2.getPlaceholder()).isEqualTo("@p1");
	}

	@Test // gh-15
	public void nextShouldConsiderNameLimit() {

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 10).create();

		BindMarker marker1 = bindMarkers.next("123456789");

		assertThat(marker1.getPlaceholder()).isEqualTo("@p012345678");
	}

	@Test // gh-15
	public void bindValueShouldBindByName() {

		BindTarget bindTarget = mock(BindTarget.class);

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		bindMarkers.next().bind(bindTarget, "foo");
		bindMarkers.next().bind(bindTarget, "bar");

		verify(bindTarget).bind("p0", "foo");
		verify(bindTarget).bind("p1", "bar");
	}

	@Test // gh-15
	public void bindNullShouldBindByName() {

		BindTarget bindTarget = mock(BindTarget.class);

		BindMarkers bindMarkers = BindMarkersFactory.named("@", "p", 32).create();

		bindMarkers.next(); // ignore
		bindMarkers.next().bindNull(bindTarget, Integer.class);

		verify(bindTarget).bindNull("p1", Integer.class);
	}
}
