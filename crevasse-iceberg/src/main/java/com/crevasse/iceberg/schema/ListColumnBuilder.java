package com.crevasse.iceberg.schema;

/**
 * Builder for list/array column types.
 */
public class ListColumnBuilder extends ColumnBuilder {
  private final ColumnType elementType;

  public ListColumnBuilder(String name, ColumnType elementType) {
    super(name);
    this.elementType = elementType;
  }

  @Override
  public ListColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  @Override
  public ListColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  @Override
  public ListColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  @Override
  public Column build() {
    return new Column(name, new ListColumnType(elementType), isOptional, doc);
  }

  /**
   * Get the element type.
   *
   * @return the type of list elements
   */
  public ColumnType getElementType() {
    return elementType;
  }
}
