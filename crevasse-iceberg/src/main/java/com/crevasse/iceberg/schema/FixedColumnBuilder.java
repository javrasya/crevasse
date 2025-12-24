package com.crevasse.iceberg.schema;

import org.apache.iceberg.types.Types;

/**
 * Builder for fixed-length binary column types.
 */
public class FixedColumnBuilder extends ColumnBuilder {
  private final int length;

  public FixedColumnBuilder(String name, int length) {
    super(name);
    this.length = length;
  }

  @Override
  public FixedColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  @Override
  public FixedColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  @Override
  public FixedColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  @Override
  public Column build() {
    return new Column(
        name, new PrimitiveColumnType(Types.FixedType.ofLength(length)), isOptional, doc);
  }

  /**
   * Get the fixed length.
   *
   * @return the length in bytes
   */
  public int getLength() {
    return length;
  }
}
