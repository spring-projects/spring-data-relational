package org.springframework.data.relational.core.dialect;

import org.springframework.data.domain.Sort;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

public interface OrderByOptionsSupport {
	String resolve(@Nullable Sort.Direction direction, @NonNull Sort.NullHandling nullHandling);
}
