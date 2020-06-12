package org.springframework.data.jdbc.repository.query.parameter;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.InParameterBinding;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.LikeParameterBinding;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.Metadata;
import org.springframework.data.jdbc.repository.query.parameter.ParameterBindings.ParameterBinding;
import org.springframework.data.repository.query.SpelQueryContext;
import org.springframework.data.repository.query.SpelQueryContext.SpelExtractor;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A parser that extracts the parameter bindings from a given query string.
 *
 * TODO This class comes from Spring Data JPA org.springframework.data.jpa.repository.query.StringQuery and should be probably moved to Spring Data Commons.
 * 
 * @author Thomas Darimont
 * @author Christopher Klein
 */
public enum ParameterBindingParser {

	INSTANCE;

	private static final String EXPRESSION_PARAMETER_PREFIX = "__$synthetic$__";
	public static final String POSITIONAL_OR_INDEXED_PARAMETER = "\\?(\\d*+(?![#\\w]))";
	// .....................................................................^ not followed by a hash or a letter.
	// .................................................................^ zero or more digits.
	// .............................................................^ start with a question mark.
	private static final Pattern PARAMETER_BINDING_BY_INDEX = Pattern.compile(POSITIONAL_OR_INDEXED_PARAMETER);
	private static final Pattern PARAMETER_BINDING_PATTERN;
	private static final String MESSAGE = "Already found parameter binding with same index / parameter name but differing binding type! "
			+ "Already have: %s, found %s! If you bind a parameter multiple times make sure they use the same binding.";
	private static final int INDEXED_PARAMETER_GROUP = 4;
	private static final int NAMED_PARAMETER_GROUP = 6;
	private static final int COMPARISION_TYPE_GROUP = 1;
	
	public static final String IDENTIFIER = "[._$[\\P{Z}&&\\P{Cc}&&\\P{Cf}&&\\P{Punct}]]+";
	public static final String COLON_NO_DOUBLE_COLON = "(?<![:\\\\]):";
	public static final String IDENTIFIER_GROUP = String.format("(%s)", IDENTIFIER);


	static {

		List<String> keywords = new ArrayList<>();

		for (ParameterBindingType type : ParameterBindingType.values()) {
			if (type.getKeyword() != null) {
				keywords.add(type.getKeyword());
			}
		}

		StringBuilder builder = new StringBuilder();
		builder.append("(");
		builder.append(StringUtils.collectionToDelimitedString(keywords, "|")); // keywords
		builder.append(")?");
		builder.append("(?: )?"); // some whitespace
		builder.append("\\(?"); // optional braces around parameters
		builder.append("(");
		builder.append("%?(" + POSITIONAL_OR_INDEXED_PARAMETER + ")%?"); // position parameter and parameter index
		builder.append("|"); // or

		// named parameter and the parameter name
		builder.append("%?(" + COLON_NO_DOUBLE_COLON + IDENTIFIER_GROUP + ")%?");

		builder.append(")");
		builder.append("\\)?"); // optional braces around parameters

		PARAMETER_BINDING_PATTERN = Pattern.compile(builder.toString(), CASE_INSENSITIVE);
	}

	/**
	 * Parses {@link ParameterBinding} instances from the given query and adds them to the registered bindings. Returns
	 * the cleaned up query.
	 */
	public String parseParameterBindingsOfQueryIntoBindingsAndReturnCleanedQuery(String query,
			List<ParameterBinding> bindings, Metadata queryMeta) {

		int greatestParameterIndex = tryFindGreatestParameterIndexIn(query);
		boolean parametersShouldBeAccessedByIndex = greatestParameterIndex != -1;

		/*
		 * Prefer indexed access over named parameters if only SpEL Expression parameters are present.
		 */
		if (!parametersShouldBeAccessedByIndex && query.contains("?#{")) {
			parametersShouldBeAccessedByIndex = true;
			greatestParameterIndex = 0;
		}

		SpelExtractor spelExtractor = createSpelExtractor(query, parametersShouldBeAccessedByIndex,
				greatestParameterIndex);

		String resultingQuery = spelExtractor.getQueryString();
		Matcher matcher = PARAMETER_BINDING_PATTERN.matcher(resultingQuery);

		int expressionParameterIndex = parametersShouldBeAccessedByIndex ? greatestParameterIndex : 0;

		boolean usesJpaStyleParameters = false;
		while (matcher.find()) {

			if (spelExtractor.isQuoted(matcher.start())) {
				continue;
			}

			String parameterIndexString = matcher.group(INDEXED_PARAMETER_GROUP);
			String parameterName = parameterIndexString != null ? null : matcher.group(NAMED_PARAMETER_GROUP);
			Integer parameterIndex = getParameterIndex(parameterIndexString);

			String typeSource = matcher.group(COMPARISION_TYPE_GROUP);
			String expression = spelExtractor.getParameter(parameterName == null ? parameterIndexString : parameterName);
			String replacement = null;

			Assert.isTrue(parameterIndexString != null || parameterName != null, () -> String.format("We need either a name or an index! Offending query string: %s", query));

			expressionParameterIndex++;
			if ("".equals(parameterIndexString)) {

				queryMeta.setUsesJdbcStyleParameters(true);
				parameterIndex = expressionParameterIndex;
			} else {
				usesJpaStyleParameters = true;
			}

			if (usesJpaStyleParameters && queryMeta.isUsesJdbcStyleParameters()) {
				throw new IllegalArgumentException("Mixing of ? parameters and other forms like ?1 is not supported!");
			}

			switch (ParameterBindingType.of(typeSource)) {

				case LIKE:

					Type likeType = LikeParameterBinding.getLikeTypeFrom(matcher.group(2));
					replacement = matcher.group(3);

					if (parameterIndex != null) {
						checkAndRegister(new LikeParameterBinding(parameterIndex, likeType, expression), bindings);
					} else {
						checkAndRegister(new LikeParameterBinding(parameterName, likeType, expression), bindings);

						replacement = expression != null ? ":" + parameterName : matcher.group(5);
					}

					break;

				case IN:

					if (parameterIndex != null) {
						checkAndRegister(new InParameterBinding(parameterIndex, expression), bindings);
					} else {
						checkAndRegister(new InParameterBinding(parameterName, expression), bindings);
					}

					break;

				case AS_IS: // fall-through we don't need a special parameter binding for the given parameter.
				default:

					bindings.add(parameterIndex != null ? new ParameterBinding(null, parameterIndex, expression)
							: new ParameterBinding(parameterName, null, expression));
			}

			if (replacement != null) {
				resultingQuery = replaceFirst(resultingQuery, matcher.group(2), replacement);
			}

		}

		return resultingQuery;
	}

	private static SpelExtractor createSpelExtractor(String queryWithSpel, boolean parametersShouldBeAccessedByIndex,
			int greatestParameterIndex) {

		/*
		 * If parameters need to be bound by index, we bind the synthetic expression parameters starting from position of the greatest discovered index parameter in order to
		 * not mix-up with the actual parameter indices.
		 */
		int expressionParameterIndex = parametersShouldBeAccessedByIndex ? greatestParameterIndex : 0;

		BiFunction<Integer, String, String> indexToParameterName = parametersShouldBeAccessedByIndex
				? (index, expression) -> String.valueOf(index + expressionParameterIndex + 1)
				: (index, expression) -> EXPRESSION_PARAMETER_PREFIX + (index + 1);

		String fixedPrefix = parametersShouldBeAccessedByIndex ? "?" : ":";

		BiFunction<String, String, String> parameterNameToReplacement = (prefix, name) -> fixedPrefix + name;

		return SpelQueryContext.of(indexToParameterName, parameterNameToReplacement).parse(queryWithSpel);
	}

	private static String replaceFirst(String text, String substring, String replacement) {

		int index = text.indexOf(substring);
		if (index < 0) {
			return text;
		}

		return text.substring(0, index) + replacement + text.substring(index + substring.length());
	}

	@Nullable
	private static Integer getParameterIndex(@Nullable String parameterIndexString) {

		if (parameterIndexString == null || parameterIndexString.isEmpty()) {
			return null;
		}
		return Integer.valueOf(parameterIndexString);
	}

	private static int tryFindGreatestParameterIndexIn(String query) {

		Matcher parameterIndexMatcher = PARAMETER_BINDING_BY_INDEX.matcher(query);

		int greatestParameterIndex = -1;
		while (parameterIndexMatcher.find()) {

			String parameterIndexString = parameterIndexMatcher.group(1);
			Integer parameterIndex = getParameterIndex(parameterIndexString);
			if (parameterIndex != null) {
				greatestParameterIndex = Math.max(greatestParameterIndex, parameterIndex);
			}
		}

		return greatestParameterIndex;
	}

	private static void checkAndRegister(ParameterBinding binding, List<ParameterBinding> bindings) {

		bindings.stream() //
				.filter(it -> it.hasName(binding.getName()) || it.hasPosition(binding.getPosition())) //
				.forEach(it -> Assert.isTrue(it.equals(binding), String.format(MESSAGE, it, binding)));

		if (!bindings.contains(binding)) {
			bindings.add(binding);
		}
	}

	/**
	 * An enum for the different types of bindings.
	 *
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 */
	private enum ParameterBindingType {

		// Trailing whitespace is intentional to reflect that the keywords must be used with at least one whitespace
		// character, while = does not.
		LIKE("like "), IN("in "), AS_IS(null);

		private final @Nullable String keyword;

		ParameterBindingType(@Nullable String keyword) {
			this.keyword = keyword;
		}

		/**
		 * Returns the keyword that will trigger the binding type or {@literal null} if the type is not triggered by a
		 * keyword.
		 *
		 * @return the keyword
		 */
		@Nullable
		public String getKeyword() {
			return keyword;
		}

		/**
		 * Return the appropriate {@link ParameterBindingType} for the given {@link String}. Returns {@literal #AS_IS} in
		 * case no other {@link ParameterBindingType} could be found.
		 */
		static ParameterBindingType of(String typeSource) {

			if (!StringUtils.hasText(typeSource)) {
				return AS_IS;
			}

			for (ParameterBindingType type : values()) {
				if (type.name().equalsIgnoreCase(typeSource.trim())) {
					return type;
				}
			}

			throw new IllegalArgumentException(String.format("Unsupported parameter binding type %s!", typeSource));
		}
	}
}
