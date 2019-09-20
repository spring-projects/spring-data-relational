package org.springframework.data.r2dbc.repository.support;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.core.DatabaseClient.BindSpec;

/**
 * Adapter for {@link BindSpec} to be used with {@link org.springframework.data.r2dbc.dialect.BindMarker} binding.
 * Binding parameters updates the {@link BindSpec}
 *
 * @param <S> type of the bind specification.
 * @author Mark Paluch
 */
class BindSpecAdapter<S extends BindSpec<S>> implements Statement {

	private S bindSpec;

	private BindSpecAdapter(S bindSpec) {
		this.bindSpec = bindSpec;
	}

	/**
	 * Create a new {@link BindSpecAdapter} for the given {@link BindSpec}.
	 *
	 * @param bindSpec the bind specification.
	 * @param <S> type of the bind spec to retain the type through {@link #getBoundOperation()}.
	 * @return {@link BindSpecAdapter} for the {@link BindSpec}.
	 */
	public static <S extends BindSpec<S>> BindSpecAdapter<S> create(S bindSpec) {
		return new BindSpecAdapter<>(bindSpec);
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#add()
	 */
	@Override
	public BindSpecAdapter<S> add() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#execute()
	 */
	@Override
	public Publisher<? extends Result> execute() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
	 */
	@Override
	public BindSpecAdapter<S> bind(String identifier, Object value) {

		this.bindSpec = bindSpec.bind(identifier, value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
	 */
	@Override
	public BindSpecAdapter<S> bind(int index, Object value) {

		this.bindSpec = bindSpec.bind(index, value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
	 */
	@Override
	public BindSpecAdapter<S> bindNull(String identifier, Class<?> type) {

		this.bindSpec = bindSpec.bindNull(identifier, type);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
	 */
	@Override
	public BindSpecAdapter<S> bindNull(int index, Class<?> type) {

		this.bindSpec = bindSpec.bindNull(index, type);
		return this;
	}

	/**
	 * @return the bound (final) bind specification.
	 */
	public S getBoundOperation() {
		return bindSpec;
	}
}
