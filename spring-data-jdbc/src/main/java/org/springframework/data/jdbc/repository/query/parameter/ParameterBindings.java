package org.springframework.data.jdbc.repository.query.parameter;

import static org.springframework.util.ObjectUtils.nullSafeEquals;
import static org.springframework.util.ObjectUtils.nullSafeHashCode;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * TODO This class comes from Spring Data JPA org.springframework.data.jpa.repository.query.StringQuery and should be probably moved to Spring Data Commons.
 * @author Christopher Klein
 */
public class ParameterBindings {

	/**
	 * A generic parameter binding with name or position information.
	 *
	 * @author Thomas Darimont
	 */
	public static class ParameterBinding {

		private final @Nullable String name;
		private final @Nullable String expression;
		private final @Nullable Integer position;

		/**
		 * Creates a new {@link ParameterBinding} for the parameter with the given
		 * position.
		 *
		 * @param position must not be {@literal null}.
		 */
		ParameterBinding(Integer position) {
			this(null, position, null);
		}

		/**
		 * Creates a new {@link ParameterBinding} for the parameter with the given name,
		 * position and expression information. Either {@literal name} or
		 * {@literal position} must be not {@literal null}.
		 *
		 * @param name       of the parameter may be {@literal null}.
		 * @param position   of the parameter may be {@literal null}.
		 * @param expression the expression to apply to any value for this parameter.
		 */
		ParameterBinding(@Nullable String name, @Nullable Integer position, @Nullable String expression) {

			if (name == null) {
				Assert.notNull(position, "Position must not be null!");
			}

			if (position == null) {
				Assert.notNull(name, "Name must not be null!");
			}

			this.name = name;
			this.position = position;
			this.expression = expression;
		}

		/**
		 * Returns whether the binding has the given name. Will always be
		 * {@literal false} in case the {@link ParameterBinding} has been set up from a
		 * position.
		 */
		boolean hasName(@Nullable String name) {
			return this.position == null && this.name != null && this.name.equals(name);
		}

		/**
		 * Returns whether the binding has the given position. Will always be
		 * {@literal false} in case the {@link ParameterBinding} has been set up from a
		 * name.
		 */
		boolean hasPosition(@Nullable Integer position) {
			return position != null && this.name == null && position.equals(this.position);
		}

		/**
		 * @return the name
		 */
		@Nullable
		public String getName() {
			return name;
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

			throw new IllegalStateException(String.format("Required name for %s not available!", this));
		}

		/**
		 * @return the position
		 */
		@Nullable
		Integer getPosition() {
			return position;
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

			throw new IllegalStateException(String.format("Required position for %s not available!", this));
		}

		/**
		 * @return {@literal true} if this parameter binding is a synthetic SpEL
		 *         expression.
		 */
		public boolean isExpression() {
			return this.expression != null;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = 17;

			result += nullSafeHashCode(this.name);
			result += nullSafeHashCode(this.position);
			result += nullSafeHashCode(this.expression);

			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ParameterBinding)) {
				return false;
			}

			ParameterBinding that = (ParameterBinding) obj;

			return nullSafeEquals(this.name, that.name) && nullSafeEquals(this.position, that.position)
					&& nullSafeEquals(this.expression, that.expression);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("ParameterBinding [name: %s, position: %d, expression: %s]", getName(), getPosition(),
					getExpression());
		}

		/**
		 * @param valueToBind value to prepare
		 */
		@Nullable
		public Object prepare(@Nullable Object valueToBind) {
			return valueToBind;
		}

		@Nullable
		public String getExpression() {
			return expression;
		}
	}

	/**
	 * Represents a {@link ParameterBinding} in a JPQL query augmented with
	 * instructions of how to apply a parameter as an {@code IN} parameter.
	 *
	 * @author Thomas Darimont
	 */
	public static class InParameterBinding extends ParameterBinding {

		/**
		 * Creates a new {@link InParameterBinding} for the parameter with the given
		 * name.
		 */
		InParameterBinding(String name, @Nullable String expression) {
			super(name, null, expression);
		}

		/**
		 * Creates a new {@link InParameterBinding} for the parameter with the given
		 * position.
		 */
		InParameterBinding(int position, @Nullable String expression) {
			super(null, position, expression);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.jpa.repository.query.StringQuery.ParameterBinding#
		 * prepare(java.lang.Object)
		 */
		@Override
		public Object prepare(@Nullable Object value) {

			if (!ObjectUtils.isArray(value)) {
				return value;
			}

			int length = Array.getLength(value);
			Collection<Object> result = new ArrayList<>(length);

			for (int i = 0; i < length; i++) {
				result.add(Array.get(value, i));
			}

			return result;
		}
	}

	/**
	 * Represents a parameter binding in a JPQL query augmented with instructions of
	 * how to apply a parameter as LIKE parameter. This allows expressions like
	 * {@code â€¦like %?1} in the JPQL query, which is not allowed by plain JPA.
	 *
	 * @author Oliver Gierke
	 * @author Thomas Darimont
	 */
	public static class LikeParameterBinding extends ParameterBinding {

		private static final List<Type> SUPPORTED_TYPES = Arrays.asList(Type.CONTAINING, Type.STARTING_WITH,
				Type.ENDING_WITH, Type.LIKE);

		private final Type type;

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * name and {@link Type}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param type must not be {@literal null}.
		 */
		LikeParameterBinding(String name, Type type) {
			this(name, type, null);
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * name and {@link Type} and parameter binding input.
		 *
		 * @param name       must not be {@literal null} or empty.
		 * @param type       must not be {@literal null}.
		 * @param expression may be {@literal null}.
		 */
		LikeParameterBinding(String name, Type type, @Nullable String expression) {

			super(name, null, expression);

			Assert.hasText(name, "Name must not be null or empty!");
			Assert.notNull(type, "Type must not be null!");

			Assert.isTrue(SUPPORTED_TYPES.contains(type), String.format("Type must be one of %s!",
					StringUtils.collectionToCommaDelimitedString(SUPPORTED_TYPES)));

			this.type = type;
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * position and {@link Type}.
		 *
		 * @param position position of the parameter in the query.
		 * @param type     must not be {@literal null}.
		 */
		LikeParameterBinding(int position, Type type) {
			this(position, type, null);
		}

		/**
		 * Creates a new {@link LikeParameterBinding} for the parameter with the given
		 * position and {@link Type}.
		 *
		 * @param position   position of the parameter in the query.
		 * @param type       must not be {@literal null}.
		 * @param expression may be {@literal null}.
		 */
		LikeParameterBinding(int position, Type type, @Nullable String expression) {

			super(null, position, expression);

			Assert.isTrue(position > 0, "Position must be greater than zero!");
			Assert.notNull(type, "Type must not be null!");

			Assert.isTrue(SUPPORTED_TYPES.contains(type), String.format("Type must be one of %s!",
					StringUtils.collectionToCommaDelimitedString(SUPPORTED_TYPES)));

			this.type = type;
		}

		/**
		 * Returns the {@link Type} of the binding.
		 *
		 * @return the type
		 */
		public Type getType() {
			return type;
		}

		/**
		 * Prepares the given raw keyword according to the like type.
		 */
		@Nullable
		@Override
		public Object prepare(@Nullable Object value) {

			if (value == null) {
				return null;
			}

			switch (type) {
			case STARTING_WITH:
				return String.format("%s%%", value.toString());
			case ENDING_WITH:
				return String.format("%%%s", value.toString());
			case CONTAINING:
				return String.format("%%%s%%", value.toString());
			case LIKE:
			default:
				return value;
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof LikeParameterBinding)) {
				return false;
			}

			LikeParameterBinding that = (LikeParameterBinding) obj;

			return super.equals(obj) && this.type.equals(that.type);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = super.hashCode();

			result += nullSafeHashCode(this.type);

			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("LikeBinding [name: %s, position: %d, type: %s]", getName(), getPosition(), type);
		}

		/**
		 * Extracts the like {@link Type} from the given JPA like expression.
		 *
		 * @param expression must not be {@literal null} or empty.
		 */
		static Type getLikeTypeFrom(String expression) {

			Assert.hasText(expression, "Expression must not be null or empty!");

			if (expression.matches("%.*%")) {
				return Type.CONTAINING;
			}

			if (expression.startsWith("%")) {
				return Type.ENDING_WITH;
			}

			if (expression.endsWith("%")) {
				return Type.STARTING_WITH;
			}

			return Type.LIKE;
		}
	}

	public static class Metadata {
		private boolean usesJdbcStyleParameters = false;
		
		public boolean isUsesJdbcStyleParameters() {
			return usesJdbcStyleParameters;
		}
		
		public void setUsesJdbcStyleParameters(boolean usesJdbcStyleParameters) {
			this.usesJdbcStyleParameters = usesJdbcStyleParameters;
		}
	}
}
