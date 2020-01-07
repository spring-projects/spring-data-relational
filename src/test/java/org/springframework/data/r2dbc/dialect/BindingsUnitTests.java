/*
 * Copyright 2019-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * Unit tests for {@link Bindings}.
 *
 * @author Mark Paluch
 */
public class BindingsUnitTests {

	BindMarkersFactory markersFactory = BindMarkersFactory.indexed("$", 1);
	BindTarget bindTarget = mock(BindTarget.class);

	@Test // gh-64
	public void shouldCreateBindings() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		bindings.bind(bindings.nextMarker(), "foo");
		bindings.bindNull(bindings.nextMarker(), String.class);

		assertThat(bindings.stream()).hasSize(2);
	}

	@Test // gh-64
	public void shouldApplyValueBinding() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		bindings.bind(bindings.nextMarker(), "foo");
		bindings.apply(bindTarget);

		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-64
	public void shouldApplySimpleValueBinding() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		BindMarker marker = bindings.bind("foo");
		bindings.apply(bindTarget);

		assertThat(marker.getPlaceholder()).isEqualTo("$1");
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-64
	public void shouldApplyNullBinding() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		bindings.bindNull(bindings.nextMarker(), String.class);

		bindings.apply(bindTarget);

		verify(bindTarget).bindNull(0, String.class);
	}

	@Test // gh-64
	public void shouldApplySimpleNullBinding() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		BindMarker marker = bindings.bindNull(String.class);
		bindings.apply(bindTarget);

		assertThat(marker.getPlaceholder()).isEqualTo("$1");
		verify(bindTarget).bindNull(0, String.class);
	}

	@Test // gh-64
	public void shouldConsumeBindings() {

		MutableBindings bindings = new MutableBindings(markersFactory.create());

		bindings.bind(bindings.nextMarker(), "foo");
		bindings.bindNull(bindings.nextMarker(), String.class);

		AtomicInteger counter = new AtomicInteger();

		bindings.forEach(binding -> {

			if (binding.hasValue()) {
				counter.incrementAndGet();
				assertThat(binding.getValue()).isEqualTo("foo");
				assertThat(binding.getBindMarker().getPlaceholder()).isEqualTo("$1");
			}

			if (binding.isNull()) {
				counter.incrementAndGet();

				assertThat(((Bindings.NullBinding) binding).getValueType()).isEqualTo(String.class);
				assertThat(binding.getBindMarker().getPlaceholder()).isEqualTo("$2");
			}
		});

		assertThat(counter).hasValue(2);
	}

	@Test // gh-64
	public void shouldMergeBindings() {

		BindMarkers markers = markersFactory.create();

		BindMarker shared = markers.next();
		BindMarker leftMarker = markers.next();
		List<Bindings.Binding> left = new ArrayList<>();
		left.add(new Bindings.NullBinding(shared, String.class));
		left.add(new Bindings.ValueBinding(leftMarker, "left"));

		BindMarker rightMarker = markers.next();
		List<Bindings.Binding> right = new ArrayList<>();
		left.add(new Bindings.ValueBinding(shared, "override"));
		left.add(new Bindings.ValueBinding(rightMarker, "right"));

		Bindings merged = Bindings.merge(new Bindings(left), new Bindings(right));

		assertThat(merged).hasSize(3);

		merged.apply(bindTarget);
		verify(bindTarget).bind(0, "override");
		verify(bindTarget).bind(1, "left");
		verify(bindTarget).bind(2, "right");
	}

}
