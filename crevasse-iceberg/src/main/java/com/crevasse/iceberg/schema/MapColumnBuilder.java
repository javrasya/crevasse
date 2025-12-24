package com.crevasse.iceberg.schema;

/**
 * Builder for map column types.
 * Supports fluent API with key/value types as constructor arguments:
 * <pre>
 * mapCol('metadata', stringType(), intType()).notNullable()
 * </pre>
 */
public class MapColumnBuilder extends ColumnBuilder {
  private final ColumnType keyType;
  private final ColumnType valueType;

  public MapColumnBuilder(String name, ColumnType keyType, ColumnType valueType) {
    super(name);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public MapColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  @Override
  public MapColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  @Override
  public MapColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  @Override
  public Column build() {
    return new Column(name, new MapColumnType(keyType, valueType), isOptional, doc);
  }

  /**
   * Get the key type.
   *
   * @return the key column type
   */
  public ColumnType getKeyType() {
    return keyType;
  }

  /**
   * Get the value type.
   *
   * @return the value column type
   */
  public ColumnType getValueType() {
    return valueType;
  }
}
