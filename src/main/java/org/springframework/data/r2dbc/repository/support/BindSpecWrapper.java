package org.springframework.data.r2dbc.repository.support;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.function.DatabaseClient.BindSpec;

/**
 * Wrapper for {@link BindSpec} to be used with {@link org.springframework.data.r2dbc.dialect.BindMarker} binding.
 * Binding parameters updates the {@link BindSpec}
 *
 * @param <S> type of the bind specification.
 * @author Mark Paluch
 */
class BindSpecWrapper<S extends BindSpec<S>> implements Statement<BindSpecWrapper<S>> {

	private S bindSpec;

	private BindSpecWrapper(S bindSpec) {
		this.bindSpec = bindSpec;
	}

	/**
	 * Create a new {@link BindSpecWrapper} for the given {@link BindSpec}.
	 *
	 * @param bindSpec the bind specification.
	 * @param <S> type of the bind spec to retain the type through {@link #getBoundOperation()}.
	 * @return {@link BindSpecWrapper} for the {@link BindSpec}.
	 */
	public static <S extends BindSpec<S>> BindSpecWrapper<S> create(S bindSpec) {
		return new BindSpecWrapper<>(bindSpec);
	}

	/* 
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#add()
	 */
	@Override
	public BindSpecWrapper<S> add() {
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
	public BindSpecWrapper<S> bind(Object identifier, Object value) {

		this.bindSpec = bindSpec.bind((String) identifier, value);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
	 */
	@Override
	public BindSpecWrapper<S> bind(int index, Object value) {

		this.bindSpec = bindSpec.bind(index, value);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
	 */
	@Override
	public BindSpecWrapper<S> bindNull(Object identifier, Class<?> type) {

		this.bindSpec = bindSpec.bindNull((String) identifier, type);
		return this;
	}

	/* 
	 * (non-Javadoc)
	 * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
	 */
	@Override
	public BindSpecWrapper<S> bindNull(int index, Class<?> type) {

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
