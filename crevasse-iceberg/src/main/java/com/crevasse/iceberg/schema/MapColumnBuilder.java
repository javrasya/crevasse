package com.crevasse.iceberg.schema;

import com.crevasse.iceberg.StructTypeHandler;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;

/**
 * Builder for map column types.
 * Supports fluent API with closure for defining key/value types:
 * <pre>
 * mapCol('metadata').nullable {
 *     key(stringType())
 *     value(intType())
 * }
 * </pre>
 */
public class MapColumnBuilder extends ColumnBuilder {
  private ColumnType keyType;
  private ColumnType valueType;

  public MapColumnBuilder(String name) {
    super(name);
  }

  @Override
  public MapColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  /**
   * Mark as nullable and define key/value types with a closure.
   *
   * @param closure the closure defining key and value types
   * @return this builder for chaining
   */
  public MapColumnBuilder nullable(
      @DelegatesTo(value = StructTypeHandler.MapTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    super.nullable();
    processKeyValueClosure(closure);
    return this;
  }

  @Override
  public MapColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  /**
   * Mark as not nullable and define key/value types with a closure.
   *
   * @param closure the closure defining key and value types
   * @return this builder for chaining
   */
  public MapColumnBuilder notNullable(
      @DelegatesTo(value = StructTypeHandler.MapTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    super.notNullable();
    processKeyValueClosure(closure);
    return this;
  }

  /**
   * Define key/value types with a closure (without changing nullability).
   *
   * @param closure the closure defining key and value types
   * @return this builder for chaining
   */
  public MapColumnBuilder keyValue(
      @DelegatesTo(value = StructTypeHandler.MapTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    processKeyValueClosure(closure);
    return this;
  }

  @Override
  public MapColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  private void processKeyValueClosure(Closure<?> closure) {
    StructTypeHandler.MapTypeHandler mapHandler = new StructTypeHandler.MapTypeHandler();
    closure.setDelegate(mapHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();
    this.keyType = mapHandler.getKeyType();
    this.valueType = mapHandler.getValueType();
  }

  @Override
  public Column build() {
    if (keyType == null || valueType == null) {
      throw new IllegalStateException(
          "MapColumnBuilder requires both key and value types. "
              + "Use nullable { key(...); value(...) } or notNullable { key(...); value(...) }");
    }
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
