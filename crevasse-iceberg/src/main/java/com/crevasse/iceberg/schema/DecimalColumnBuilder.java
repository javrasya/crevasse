package com.crevasse.iceberg.schema;

import org.apache.iceberg.types.Types;

/**
 * Builder for decimal column types with precision and scale.
 */
public class DecimalColumnBuilder extends ColumnBuilder {
  private final int precision;
  private final int scale;

  public DecimalColumnBuilder(String name, int precision, int scale) {
    super(name);
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public DecimalColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  @Override
  public DecimalColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  @Override
  public DecimalColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  @Override
  public Column build() {
    return new Column(
        name, new PrimitiveColumnType(Types.DecimalType.of(precision, scale)), isOptional, doc);
  }

  /**
   * Get the precision.
   *
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the scale.
   *
   * @return the scale
   */
  public int getScale() {
    return scale;
  }
}
