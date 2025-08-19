package org.springframework.data.jdbc.repository.aot;

import java.util.List;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.repository.query.ParametrizedQuery;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * PartTree (derived) Query with a limit associated.
 *
 * @author Mark Paluch
 */
class DerivedAotQuery extends StringAotQuery {

	private final String queryString;
	private final CriteriaDefinition criteria;
	private final Sort sort;
	private final Limit limit;
	private final boolean delete;
	private final boolean count;
	private final boolean exists;

	DerivedAotQuery(String queryString, CriteriaDefinition criteria, Sort sort,
			Limit limit, boolean delete, boolean count, boolean exists) {
		super(List.of());
		this.queryString = queryString;
		this.criteria = criteria;
		this.sort = sort;
		this.limit = limit;
		this.delete = delete;
		this.count = count;
		this.exists = exists;
	}

	DerivedAotQuery(ParametrizedQuery query, PartTree partTree, boolean countQuery) {

		this(query.getQuery(), query.getCriteria(), partTree.getSort(), partTree.getResultLimit(), partTree.isDelete(),
				countQuery, partTree.isExistsProjection());

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
	public boolean isCount() {
		return count;
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
