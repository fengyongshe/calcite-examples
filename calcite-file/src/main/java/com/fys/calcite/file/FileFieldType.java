package com.fys.calcite.file;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Map;

public enum FileFieldType {

  STRING(null, String.class),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(null, java.sql.Date.class),
  TIME(null, java.sql.Time.class),
  TIMESTAMP(null, java.sql.Timestamp.class);

  private final Primitive primitive;
  private final Class clazz;

  private static final Map<String, FileFieldType> MAP;

  static {
    ImmutableMap.Builder<String, FileFieldType> builder =
        ImmutableMap.builder();
    for (FileFieldType value : values()) {
      builder.put(value.clazz.getSimpleName(), value);

      if (value.primitive != null) {
        builder.put(value.primitive.primitiveClass.getSimpleName(), value);
      }
    }
    MAP = builder.build();
  }

  FileFieldType(Primitive primitive) {
    this(primitive, primitive.boxClass);
  }

  FileFieldType(Primitive primitive, Class clazz) {
    this.primitive = primitive;
    this.clazz = clazz;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    return typeFactory.createJavaType(clazz);
  }

  public static FileFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}
