/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.springframework.util.ObjectUtils.*;

import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Score;
import org.springframework.data.domain.Vector;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.repository.query.Parameter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A generic parameter binding with name or position information.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class ParameterBinding {

	private final BindingIdentifier identifier;
	private final ParameterOrigin origin;

	/**
	 * Creates a new {@link ParameterBinding} for the parameter with the given identifier and origin.
	 *
	 * @param identifier of the parameter, must not be {@literal null}.
	 * @param origin the origin of the parameter (expression or method argument)
	 */
	ParameterBinding(BindingIdentifier identifier, ParameterOrigin origin) {

		Assert.notNull(identifier, "BindingIdentifier must not be null");
		Assert.notNull(origin, "ParameterOrigin must not be null");

		this.identifier = identifier;
		this.origin = origin;
	}

	/**
	 * Creates a new {@link ParameterBinding} for the parameter with the given name and origin.
	 *
	 * @param parameter
	 * @return
	 */
	public static ParameterBinding of(Parameter parameter) {
		return named(parameter.getRequiredName(), ParameterOrigin.ofParameter(parameter));
	}

	/**
	 * Creates a new {@link ParameterBinding} for the named parameter with the given name and origin.
	 *
	 * @param name
	 * @param origin
	 * @return
	 */
	public static ParameterBinding named(String name, ParameterOrigin origin) {
		return new ParameterBinding(BindingIdentifier.of(name), origin);
	}

	public BindingIdentifier getIdentifier() {
		return identifier;
	}

	public ParameterOrigin getOrigin() {
		return origin;
	}

	/**
	 * @return the name if available or {@literal null}.
	 */
	public @Nullable String getName() {
		return identifier.hasName() ? identifier.getName() : null;
	}

	/**
	 * @return {@literal true} if the binding identifier is associated with a name.
	 * @since 4.0
	 */
	boolean hasName() {
		return identifier.hasName();
	}

	/**
	 * @return the name
	 * @throws IllegalStateException if the name is not available.
	 * @since 2.0
	 */
	String getRequiredName() throws IllegalStateException {

		String name = getName();

		if (name != null) {
			return name;
		}

		throw new IllegalStateException(String.format("Required name for %s not available", this));
	}

	/**
	 * @return the position if available or {@literal null}.
	 */
	@Nullable
	Integer getPosition() {
		return identifier.hasPosition() ? identifier.getPosition() : null;
	}

	/**
	 * @return the position
	 * @throws IllegalStateException if the position is not available.
	 * @since 2.0
	 */
	int getRequiredPosition() throws IllegalStateException {

		Integer position = getPosition();

		if (position != null) {
			return position;
		}

		throw new IllegalStateException(String.format("Required position for %s not available", this));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ParameterBinding that = (ParameterBinding) o;

		if (!nullSafeEquals(identifier, that.identifier)) {
			return false;
		}
		return nullSafeEquals(origin, that.origin);
	}

	@Override
	public int hashCode() {
		int result = nullSafeHashCode(identifier);
		result = 31 * result + nullSafeHashCode(origin);
		return result;
	}

	@Override
	public String toString() {
		return String.format("ParameterBinding [identifier: %s, origin: %s]", identifier, origin);
	}

	/**
	 * @param valueToBind value to prepare
	 */
	public @Nullable Object prepare(@Nullable Object valueToBind) {

		if (valueToBind instanceof Score score) {
			return score.getValue();
		}

		if (valueToBind instanceof Vector v) {
			return v.getType() == Float.TYPE ? v.toFloatArray() : v.toDoubleArray();
		}

		return valueToBind;
	}

	/**
	 * Check whether the {@code other} binding uses the same bind target.
	 *
	 * @param other must not be {@literal null}.
	 * @return {@code true} if the other binding uses the same parameter to bind to as this one.
	 */
	public boolean bindsTo(ParameterBinding other) {

		if (getIdentifier().equals(other.getIdentifier())) {
			return true;
		}

		if (identifier.hasName() && other.identifier.hasName()) {
			if (identifier.getName().equals(other.identifier.getName())) {
				return true;
			}
		}

		if (identifier.hasPosition() && other.identifier.hasPosition()) {
			if (identifier.getPosition() == other.identifier.getPosition()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Check whether this binding can be bound as the {@code other} binding by checking its type and origin. Subclasses
	 * may override this method to include other properties for the compatibility check.
	 *
	 * @param other
	 * @return {@code true} if the other binding is compatible with this one.
	 */
	public boolean isCompatibleWith(ParameterBinding other) {
		return other.getClass() == getClass() && other.getOrigin().equals(getOrigin());
	}

	/**
	 * Identifies a binding parameter by name, position or both. Used to bind parameters to a query or to describe a
	 * {@link MethodInvocationArgument} origin.
	 *
	 * @author Mark Paluch
	 * @since 3.1.2
	 */
	public sealed interface BindingIdentifier permits Named, ParameterBinding.Indexed, NamedAndIndexed {

		/**
		 * Creates an identifier for the given {@code name}.
		 *
		 * @param name
		 * @return
		 */
		static BindingIdentifier of(String name) {

			Assert.hasText(name, "Name must not be empty");

			return new Named(name);
		}

		/**
		 * Creates an identifier for the given {@code position}.
		 *
		 * @param position 1-based index.
		 * @return
		 */
		static BindingIdentifier of(int position) {

			Assert.isTrue(position > -1, "Index position must be greater zero");

			return new Indexed(position);
		}

		/**
		 * Creates an identifier for the given {@code name} and {@code position}.
		 *
		 * @param name
		 * @return
		 */
		static BindingIdentifier of(String name, int position) {

			Assert.hasText(name, "Name must not be empty");

			return new NamedAndIndexed(name, position);
		}

		/**
		 * @return {@code true} if the binding is associated with a name.
		 */
		default boolean hasName() {
			return false;
		}

		/**
		 * @return {@code true} if the binding is associated with a position index.
		 */
		default boolean hasPosition() {
			return false;
		}

		/**
		 * Returns the binding name {@link #hasName() if present} or throw {@link IllegalStateException} if no name
		 * associated.
		 *
		 * @return the binding name.
		 */
		default String getName() {
			throw new IllegalStateException("No name associated");
		}

		/**
		 * Returns the binding name {@link #hasPosition() if present} or throw {@link IllegalStateException} if no position
		 * associated.
		 *
		 * @return the binding position.
		 */
		default int getPosition() {
			throw new IllegalStateException("No position associated");
		}

		/**
		 * Map the name of the binding to a new name using the given {@link Function} if the binding has a name. If the
		 * binding is not associated with a name, then the binding is returned unchanged.
		 *
		 * @param nameMapper must not be {@literal null}.
		 * @return the transformed {@link BindingIdentifier} if the binding has a name, otherwise the binding itself.
		 * @since 4.0
		 */
		BindingIdentifier mapName(Function<? super String, ? extends String> nameMapper);

		/**
		 * Associate a position with the binding.
		 *
		 * @param position
		 * @return the new binding identifier with the position.
		 * @since 4.0
		 */
		BindingIdentifier withPosition(int position);

	}

	private record Named(String name) implements BindingIdentifier {

		@Override
		public boolean hasName() {
			return true;
		}

		@Override
		public String getName() {
			return name();
		}

		@Override
		public String toString() {
			return name();
		}

		@Override
		public BindingIdentifier mapName(Function<? super String, ? extends String> nameMapper) {
			return new Named(nameMapper.apply(name()));
		}

		@Override
		public BindingIdentifier withPosition(int position) {
			return new NamedAndIndexed(name, position);
		}
	}

	private record Indexed(int position) implements BindingIdentifier {

		@Override
		public boolean hasPosition() {
			return true;
		}

		@Override
		public int getPosition() {
			return position();
		}

		@Override
		public BindingIdentifier mapName(Function<? super String, ? extends String> nameMapper) {
			return this;
		}

		@Override
		public BindingIdentifier withPosition(int position) {
			return new Indexed(position);
		}

		@Override
		public String toString() {
			return "[" + position() + "]";
		}
	}

	private record NamedAndIndexed(String name, int position) implements BindingIdentifier {

		@Override
		public boolean hasName() {
			return true;
		}

		@Override
		public String getName() {
			return name();
		}

		@Override
		public boolean hasPosition() {
			return true;
		}

		@Override
		public int getPosition() {
			return position();
		}

		@Override
		public BindingIdentifier mapName(Function<? super String, ? extends String> nameMapper) {
			return new NamedAndIndexed(nameMapper.apply(name), position);
		}

		@Override
		public BindingIdentifier withPosition(int position) {
			return new NamedAndIndexed(name, position);
		}

		@Override
		public String toString() {
			return "[" + name() + ", " + position() + "]";
		}
	}

	/**
	 * Value type hierarchy to describe where a binding parameter comes from, either method call or an expression.
	 *
	 * @author Mark Paluch
	 * @since 3.1.2
	 */
	public sealed interface ParameterOrigin permits Expression, MethodInvocationArgument, Synthetic {

		/**
		 * Creates a {@link Expression} for the given {@code expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return {@link Expression} for the given {@code expression}.
		 */
		static Expression ofExpression(ValueExpression expression) {
			return new Expression(expression);
		}

		/**
		 * Creates a {@link Expression} for the given {@code expression} string.
		 *
		 * @param value the captured value.
		 * @param source source from which this value is derived.
		 * @return {@link Synthetic} for the given {@code value}.
		 */
		static Synthetic synthetic(@Nullable Object value, Object source) {
			return new Synthetic(value, source);
		}

		/**
		 * Creates a {@link MethodInvocationArgument} object for {@code name}
		 *
		 * @param name the parameter name from the method invocation.
		 * @return {@link MethodInvocationArgument} object for {@code name}.
		 */
		static MethodInvocationArgument ofParameter(String name) {
			return ofParameter(name, null);
		}

		/**
		 * Creates a {@link MethodInvocationArgument} object for {@code name} and {@code position}. Either the name or the
		 * position must be given.
		 *
		 * @param name the parameter name from the method invocation, can be {@literal null}.
		 * @param position the parameter position (1-based) from the method invocation, can be {@literal null}.
		 * @return {@link MethodInvocationArgument} object for {@code name} and {@code position}.
		 */
		static MethodInvocationArgument ofParameter(@Nullable String name, @Nullable Integer position) {

			BindingIdentifier identifier;
			if (!ObjectUtils.isEmpty(name) && position != null) {
				identifier = BindingIdentifier.of(name, position);
			} else if (!ObjectUtils.isEmpty(name)) {
				identifier = BindingIdentifier.of(name);
			} else if (position != null) {
				identifier = BindingIdentifier.of(position);
			} else {
				throw new IllegalStateException("Neither name nor position available for binding");
			}

			return ofParameter(identifier);
		}

		/**
		 * Creates a {@link MethodInvocationArgument} object for {@code position}.
		 *
		 * @param parameter the parameter from the method invocation.
		 * @return {@link MethodInvocationArgument} object for {@code position}.
		 */
		static MethodInvocationArgument ofParameter(Parameter parameter) {
			return ofParameter(parameter.getIndex() + 1);
		}

		/**
		 * Creates a {@link MethodInvocationArgument} object for {@code position}.
		 *
		 * @param position the parameter position (1-based) from the method invocation.
		 * @return {@link MethodInvocationArgument} object for {@code position}.
		 */
		static MethodInvocationArgument ofParameter(int position) {
			return ofParameter(BindingIdentifier.of(position));
		}

		/**
		 * Creates a {@link MethodInvocationArgument} using {@link BindingIdentifier}.
		 *
		 * @param identifier must not be {@literal null}.
		 * @return {@link MethodInvocationArgument} for {@link BindingIdentifier}.
		 */
		static MethodInvocationArgument ofParameter(BindingIdentifier identifier) {
			return new MethodInvocationArgument(identifier);
		}

		/**
		 * @return {@code true} if the origin is a method argument reference.
		 */
		boolean isMethodArgument();

		/**
		 * @return {@code true} if the origin is an expression.
		 */
		boolean isExpression();

		/**
		 * @return {@code true} if the origin is synthetic (contributed by e.g. KeysetPagination)
		 */
		boolean isSynthetic();
	}

	/**
	 * Value object capturing the expression of which a binding parameter originates.
	 *
	 * @param expression
	 * @author Mark Paluch
	 * @since 3.1.2
	 */
	public record Expression(ValueExpression expression) implements ParameterOrigin {

		@Override
		public boolean isMethodArgument() {
			return false;
		}

		@Override
		public boolean isExpression() {
			return true;
		}

		@Override
		public boolean isSynthetic() {
			return true;
		}
	}

	/**
	 * Value object capturing the expression of which a binding parameter originates.
	 *
	 * @param value
	 * @param source
	 * @author Mark Paluch
	 */
	public record Synthetic(@Nullable Object value, Object source) implements ParameterOrigin {

		@Override
		public boolean isMethodArgument() {
			return false;
		}

		@Override
		public boolean isExpression() {
			return false;
		}

		@Override
		public boolean isSynthetic() {
			return true;
		}
	}

	/**
	 * Value object capturing the method invocation parameter reference.
	 *
	 * @param identifier
	 * @author Mark Paluch
	 * @since 3.1.2
	 */
	public record MethodInvocationArgument(BindingIdentifier identifier) implements ParameterOrigin {

		@Override
		public boolean isMethodArgument() {
			return true;
		}

		@Override
		public boolean isExpression() {
			return false;
		}

		@Override
		public boolean isSynthetic() {
			return false;
		}
	}
}
