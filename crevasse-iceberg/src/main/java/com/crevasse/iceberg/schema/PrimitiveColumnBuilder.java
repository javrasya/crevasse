package com.crevasse.iceberg.schema;

import org.apache.iceberg.types.Type;

/**
 * Builder for primitive column types (string, int, long, float, double, boolean,
 * date, time, timestamp, binary, uuid).
 */
public class PrimitiveColumnBuilder extends ColumnBuilder {
  private final Type.PrimitiveType primitiveType;

  public PrimitiveColumnBuilder(String name, Type.PrimitiveType primitiveType) {
    super(name);
    this.primitiveType = primitiveType;
  }

  @Override
  public PrimitiveColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  @Override
  public PrimitiveColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  @Override
  public PrimitiveColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  @Override
  public Column build() {
    return new Column(name, new PrimitiveColumnType(primitiveType), isOptional, doc);
  }

  /**
   * Get the underlying primitive type.
   *
   * @return the Iceberg primitive type
   */
  public Type.PrimitiveType getPrimitiveType() {
    return primitiveType;
  }
}
