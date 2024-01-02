/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.sql.render.DelegatingVisitor.Delegation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.AbstractTestSegment;
import org.springframework.data.relational.core.sql.Segment;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Unit tests for {@link org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor}.
 *
 * @author Jens Schauder
 */
class TypedSubtreeVisitorUnitTests {

	List<String> events = new ArrayList<>();

	@Test // GH-1003
	void enterAndLeavesSingleSegment() {

		TypedSubtreeVisitor<TestSegment> visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root = new TestSegment("root");

		root.visit(visitor);

		assertThat(events).containsExactly("enter matched root", "leave matched root");
	}

	@Test // GH-1003
	void enterAndLeavesChainOfMatchingSegmentsAsNested() {

		TypedSubtreeVisitor<TestSegment> visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root = new TestSegment("root", new TestSegment("level 1", new TestSegment("level 2")));

		root.visit(visitor);

		assertThat(events).containsExactly("enter matched root", "enter nested level 1", "enter nested level 2",
				"leave nested level 2", "leave nested level 1", "leave matched root");
	}

	@Test // GH-1003
	void enterAndLeavesMatchingChildrenAsNested() {

		TypedSubtreeVisitor<TestSegment> visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root = new TestSegment("root", new TestSegment("child 1"), new TestSegment("child 2"));

		root.visit(visitor);

		assertThat(events).containsExactly("enter matched root", "enter nested child 1", "leave nested child 1",
				"enter nested child 2", "leave nested child 2", "leave matched root");
	}

	@Test // GH-1003
	void enterAndLeavesChainOfOtherSegmentsAsNested() {

		TypedSubtreeVisitor<TestSegment> visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root = new TestSegment("root", new OtherSegment("level 1", new OtherSegment("level 2")));

		root.visit(visitor);

		assertThat(events).containsExactly("enter matched root", "enter nested level 1", "enter nested level 2",
				"leave nested level 2", "leave nested level 1", "leave matched root");
	}

	@Test // GH-1003
	void enterAndLeavesOtherChildrenAsNested() {

		TypedSubtreeVisitor<TestSegment> visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root = new TestSegment("root", new OtherSegment("child 1"), new OtherSegment("child 2"));

		root.visit(visitor);

		assertThat(events).containsExactly("enter matched root", "enter nested child 1", "leave nested child 1",
				"enter nested child 2", "leave nested child 2", "leave matched root");
	}

	@Test // GH-1003
	void visitorIsReentrant() {

		LoggingTypedSubtreeVisitor visitor = new LoggingTypedSubtreeVisitor();
		TestSegment root1 = new TestSegment("root 1");
		TestSegment root2 = new TestSegment("root 2");

		root1.visit(visitor);
		root2.visit(visitor);

		assertThat(events).containsExactly("enter matched root 1", "leave matched root 1", "enter matched root 2",
				"leave matched root 2");
	}

	@Test // GH-1003
	void delegateToOtherVisitorOnEnterMatchedRevisitsTheSegment() {

		LoggingTypedSubtreeVisitor first = new LoggingTypedSubtreeVisitor("first ");
		LoggingTypedSubtreeVisitor second = new LoggingTypedSubtreeVisitor("second ");
		first.enterMatched(s -> delegateTo(second));
		TestSegment root = new TestSegment("root", new TestSegment("child 1"), new TestSegment("child 2"));

		root.visit(first);

		assertThat(events).containsExactly("first enter matched root", "second enter matched root",
				"second enter nested child 1", "second leave nested child 1", "second enter nested child 2",
				"second leave nested child 2", "second leave matched root", "first leave matched root");
	}

	@Test // GH-1003
	void delegateToOtherVisitorOnEnterNestedRevisitsTheNestedSegment() {

		LoggingTypedSubtreeVisitor first = new LoggingTypedSubtreeVisitor("first ");
		LoggingTypedSubtreeVisitor second = new LoggingTypedSubtreeVisitor("second ");
		first.enterNested(
				s -> ((TestSegment) s).name.equals("child 2") ? delegateTo(second) : DelegatingVisitor.Delegation.retain());
		TestSegment root = new TestSegment("root", new TestSegment("child 1"), new TestSegment("child 2"),
				new TestSegment("child 3"));

		root.visit(first);

		assertThat(events).containsExactly("first enter matched root", "first enter nested child 1",
				"first leave nested child 1", "first enter nested child 2", "second enter matched child 2",
				"second leave matched child 2", "first leave nested child 2", "first enter nested child 3",
				"first leave nested child 3", "first leave matched root");
	}

	static class TestSegment extends AbstractTestSegment {

		private String name;

		TestSegment(String name, Segment... children) {

			super(children);
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	static class OtherSegment extends AbstractTestSegment {

		private String name;

		public OtherSegment(String name, Segment... children) {

			super(children);
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	class LoggingTypedSubtreeVisitor extends TypedSubtreeVisitor<TestSegment> {

		String prefix;
		Function<TestSegment, Delegation> enterMatchedDelegation;
		Function<Visitable, Delegation> enterNestedDelegation;

		LoggingTypedSubtreeVisitor(String prefix) {
			this.prefix = prefix;
		}

		LoggingTypedSubtreeVisitor() {
			this("");
		}

		@Override
		Delegation enterMatched(TestSegment segment) {

			events.add(prefix + "enter matched " + segment);
			Delegation delegation = super.enterMatched(segment);

			return enterMatchedDelegation == null ? delegation : enterMatchedDelegation.apply(segment);
		}

		void enterMatched(Function<TestSegment, Delegation> delegation) {
			enterMatchedDelegation = delegation;
		}

		@Override
		Delegation leaveMatched(TestSegment segment) {

			events.add(prefix + "leave matched " + segment);
			return super.leaveMatched(segment);
		}

		@Override
		Delegation enterNested(Visitable segment) {

			events.add(prefix + "enter nested " + segment);
			return enterNestedDelegation == null ? super.enterNested(segment) : enterNestedDelegation.apply(segment);
		}

		void enterNested(Function<Visitable, Delegation> delegation) {
			enterNestedDelegation = delegation;
		}

		@Override
		Delegation leaveNested(Visitable segment) {

			events.add(prefix + "leave nested " + segment);
			return super.leaveNested(segment);
		}

	}
}
