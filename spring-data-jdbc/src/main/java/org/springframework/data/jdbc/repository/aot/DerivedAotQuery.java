package org.springframework.data.jdbc.repository.aot;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.repository.query.ParameterBinding;
import org.springframework.data.relational.core.query.CriteriaDefinition;

/**
 * PartTree (derived) Query with a limit associated.
 *
 * @author Mark Paluch
 */
public class DerivedAotQuery extends StringAotQuery {

	private final String queryString;
	private final CriteriaDefinition criteria;
	private final Sort sort;
	private final Limit limit;
	private final boolean delete;
	private final boolean exists;

	DerivedAotQuery(String queryString, List<ParameterBinding> parameterBindings, CriteriaDefinition criteria, Sort sort,
			Limit limit, boolean delete, boolean exists) {
		super(parameterBindings);
		this.queryString = queryString;
		this.criteria = criteria;
		this.sort = sort;
		this.limit = limit;
		this.delete = delete;
		this.exists = exists;
	}

	@Override
	public String getQueryString() {
		return queryString;
	}

	@Override
	public Limit getLimit() {
		return limit;
	}

	@Override
	public boolean isDelete() {
		return delete;
	}

	@Override
	public boolean isExists() {
		return exists;
	}

	public CriteriaDefinition getCriteria() {
		return criteria;
	}

	public Sort getSort() {
		return sort;
	}
}
