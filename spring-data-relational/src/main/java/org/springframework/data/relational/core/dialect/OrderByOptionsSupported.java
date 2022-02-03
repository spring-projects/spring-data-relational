package org.springframework.data.relational.core.dialect;

import java.util.StringJoiner;

import org.springframework.data.domain.Sort;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

public enum OrderByOptionsSupported implements OrderByOptionsSupport {
	NULL_HANDLING(true),
	DEFAULT(false);

	private final boolean supportNullHandling;

	OrderByOptionsSupported(boolean supportNullHandling) {
		this.supportNullHandling = supportNullHandling;
	}

	@Override
	public String resolve(@Nullable Sort.Direction direction, @NonNull Sort.NullHandling nullHandling) {
		StringJoiner stringJoiner = new StringJoiner(" ");
		if (direction != null) {
			stringJoiner.add(direction.toString());
		}
		if (supportNullHandling && !Sort.NullHandling.NATIVE.equals(nullHandling)) {
			stringJoiner.add(nullHandling.toString().replace("_", " "));
		}
		return stringJoiner.toString();
	}
}
