/*
 * Copyright 2017-2019 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;

import org.junit.Test;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for {@link VersionAccessor}.
 *
 * @author Tom Hombergs
 */
public class VersionAccessorUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();

	@Test // DATAJDBC-219
	public void supportsPrimitiveIntVersionType() {
		VersionAccessor<IntVersion> versionAccessor = new VersionAccessor(new IntVersion(1),
				context.getRequiredPersistentEntity(IntVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo(1);
		assertThat(versionAccessor.nextVersion()).isEqualTo(2);
		versionAccessor.setVersion(3);
		assertThat(versionAccessor.currentVersion()).isEqualTo(3);
	}

	@Test // DATAJDBC-219
	public void supportsPrimitiveShortVersionType() {
		VersionAccessor<PrimitiveShortVersion> versionAccessor = new VersionAccessor(new PrimitiveShortVersion((short) 1),
				context.getRequiredPersistentEntity(PrimitiveShortVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo((short) 1);
		assertThat(versionAccessor.nextVersion()).isEqualTo((short) 2);
		versionAccessor.setVersion((short) 3);
		assertThat(versionAccessor.currentVersion()).isEqualTo((short) 3);
	}

	@Test // DATAJDBC-219
	public void supportsPrimitiveLongVersionType() {
		VersionAccessor<PrimitiveLongVersion> versionAccessor = new VersionAccessor(new PrimitiveLongVersion(1L),
				context.getRequiredPersistentEntity(PrimitiveLongVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo(1L);
		assertThat(versionAccessor.nextVersion()).isEqualTo(2L);
		versionAccessor.setVersion(3L);
		assertThat(versionAccessor.currentVersion()).isEqualTo(3L);
	}

	@Test // DATAJDBC-219
	public void supportsShortVersionType() {
		VersionAccessor<PrimitiveShortVersion> versionAccessor = new VersionAccessor(new ShortVersion((short) 1),
				context.getRequiredPersistentEntity(ShortVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo((short) 1);
		assertThat(versionAccessor.nextVersion()).isEqualTo((short) 2);
		versionAccessor.setVersion((short) 3);
		assertThat(versionAccessor.currentVersion()).isEqualTo((short) 3);
	}

	@Test // DATAJDBC-219
	public void supportsNullShortVersionType() {
		VersionAccessor<PrimitiveShortVersion> versionAccessor = new VersionAccessor(new ShortVersion(null),
				context.getRequiredPersistentEntity(ShortVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo((short) 0);
		assertThat(versionAccessor.nextVersion()).isEqualTo((short) 1);
	}

	@Test // DATAJDBC-219
	public void supportsIntegerVersionType() {
		VersionAccessor<IntegerVersion> versionAccessor = new VersionAccessor(new IntegerVersion(1),
				context.getRequiredPersistentEntity(IntegerVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo(1);
		assertThat(versionAccessor.nextVersion()).isEqualTo(2);
		versionAccessor.setVersion(3);
		assertThat(versionAccessor.currentVersion()).isEqualTo(3);
	}

	@Test // DATAJDBC-219
	public void supportsNullIntegerVersionType() {
		VersionAccessor<IntegerVersion> versionAccessor = new VersionAccessor(new IntegerVersion(null),
				context.getRequiredPersistentEntity(IntegerVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo((Integer) 0);
		assertThat(versionAccessor.nextVersion()).isEqualTo((Integer) 1);
	}

	@Test // DATAJDBC-219
	public void supportsLongVersionType() {
		VersionAccessor<LongVersion> versionAccessor = new VersionAccessor(new LongVersion(1L),
				context.getRequiredPersistentEntity(LongVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo(1L);
		assertThat(versionAccessor.nextVersion()).isEqualTo(2L);
		versionAccessor.setVersion(3L);
		assertThat(versionAccessor.currentVersion()).isEqualTo(3L);
	}

	@Test // DATAJDBC-219
	public void supportsNullLongVersionType() {
		VersionAccessor<LongVersion> versionAccessor = new VersionAccessor(new LongVersion(null),
				context.getRequiredPersistentEntity(LongVersion.class));
		assertThat(versionAccessor.currentVersion()).isEqualTo((Long) 0L);
		assertThat(versionAccessor.nextVersion()).isEqualTo((Long) 1L);
	}

	@Test // DATAJDBC-219
	public void doesNotSupportInvalidVersionType() {
		VersionAccessor<StringVersion> versionAccessor = new VersionAccessor(new StringVersion(null),
				context.getRequiredPersistentEntity(StringVersion.class));
		assertThatThrownBy(() -> versionAccessor.currentVersion()).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Invalid type for @Version field");
		assertThatThrownBy(() -> versionAccessor.nextVersion()).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Invalid type for @Version field");
	}

	@Data
	@AllArgsConstructor
	static class IntVersion {
		@Version private int version;
	}

	@Data
	@AllArgsConstructor
	static class IntegerVersion {
		@Version private Integer version;
	}

	@Data
	@AllArgsConstructor
	static class PrimitiveShortVersion {
		@Version private short version;
	}

	@Data
	@AllArgsConstructor
	static class ShortVersion {
		@Version private Short version;
	}

	@Data
	@AllArgsConstructor
	static class PrimitiveLongVersion {
		@Version private long version;
	}

	@Data
	@AllArgsConstructor
	static class LongVersion {
		@Version private Long version;
	}

	@Data
	@AllArgsConstructor
	static class StringVersion {
		@Version private String version;
	}

}
