/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for the {@link SelectBuilder}.
 *
 * @author Jens Schauder
 */
public class SelectBuilderUnitTests {

	@Test // DATAJDBC-112
	public void simplestSelect() {

		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.tableAlias("mytable").column("mycolumn").as("myalias")) //
				.build();

		assertThat(sql).isEqualTo("SELECT mytable.mycolumn AS myalias FROM mytable");
	}

	@Test // DATAJDBC-112
	public void columnWithoutTableAlias() {

		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.column("mycolumn").as("myalias")) //
				.build();

		assertThat(sql).isEqualTo("SELECT mycolumn AS myalias FROM mytable");
	}

	@Test // DATAJDBC-112
	public void whereClause() {

		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.tableAlias("mytable").column("mycolumn").as("myalias")) //
				.where(cb -> cb.tableAlias("mytable").column("mycolumn").eq().variable("var")).build();

		assertThat(sql).isEqualTo("SELECT mytable.mycolumn AS myalias FROM mytable WHERE mytable.mycolumn = :var");
	}

	@Test // DATAJDBC-112
	public void multipleColumnsSelect() {

		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.tableAlias("mytable").column("one").as("oneAlias")) //
				.column(cb -> cb.tableAlias("mytable").column("two").as("twoAlias")) //
				.build();

		assertThat(sql).isEqualTo("SELECT mytable.one AS oneAlias, mytable.two AS twoAlias FROM mytable");
	}

	@Test // DATAJDBC-112
	public void join() {
		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.tableAlias("mytable").column("mycolumn").as("myalias")) //
				.join(jb -> jb.table("other").as("o").where("oid").eq().column("mytable", "id")).build();

		assertThat(sql).isEqualTo("SELECT mytable.mycolumn AS myalias FROM mytable JOIN other AS o ON o.oid = mytable.id");
	}

	@Test // DATAJDBC-112
	public void outerJoin() {
		String sql = new SelectBuilder("mytable") //
				.column(cb -> cb.tableAlias("mytable").column("mycolumn").as("myalias")) //
				.join(jb -> jb.rightOuter().table("other").as("o").where("oid").eq().column("mytable", "id")).build();

		assertThat(sql)
				.isEqualTo("SELECT mytable.mycolumn AS myalias FROM mytable RIGHT OUTER JOIN other AS o ON o.oid = mytable.id");
	}

}
