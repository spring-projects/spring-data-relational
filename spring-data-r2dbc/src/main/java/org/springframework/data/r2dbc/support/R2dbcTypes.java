package org.springframework.data.r2dbc.support;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;
import org.springframework.util.Assert;

/**
 * Utility class to help with operations with {@link io.r2dbc.spi.Type}
 *
 * @author Mikhail Polivakha
 */
public class R2dbcTypes {

  /**
   * Creates a new {@link Type} instance based on passed {@link Class}
   *
   * @param clazz to create type over
   * @return type instance
   */
  public static Type fromClass(Class<?> clazz) {
    return new ClassBasedR2dbcType(clazz);
  }

  private record ClassBasedR2dbcType(Class<?> javaType) implements Type {

    ClassBasedR2dbcType {
      Assert.notNull(javaType, "JavaType cannot be null");
    }

    @Override
    public Class<?> getJavaType() {
      return javaType;
    }

    @Override
    public String getName() {
      return javaType.getName();
    }
  }
}
