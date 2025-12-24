package com.crevasse.iceberg.schema;

import com.crevasse.iceberg.StructTypeHandler;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for struct (nested record) column types.
 * Supports fluent API with closure for defining nested fields:
 * <pre>
 * structCol('address').nullable {
 *     stringCol('street')
 *     stringCol('city').notNullable()
 * }
 * </pre>
 */
public class StructColumnBuilder extends ColumnBuilder {
  private List<Column> nestedColumns = new ArrayList<>();

  public StructColumnBuilder(String name) {
    super(name);
  }

  @Override
  public StructColumnBuilder nullable() {
    super.nullable();
    return this;
  }

  /**
   * Mark as nullable and define nested fields with a closure.
   *
   * @param closure the closure defining nested columns
   * @return this builder for chaining
   */
  public StructColumnBuilder nullable(
      @DelegatesTo(value = StructTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    super.nullable();
    processNestedClosure(closure);
    return this;
  }

  @Override
  public StructColumnBuilder notNullable() {
    super.notNullable();
    return this;
  }

  /**
   * Mark as not nullable and define nested fields with a closure.
   *
   * @param closure the closure defining nested columns
   * @return this builder for chaining
   */
  public StructColumnBuilder notNullable(
      @DelegatesTo(value = StructTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    super.notNullable();
    processNestedClosure(closure);
    return this;
  }

  /**
   * Define nested fields with a closure (without changing nullability).
   *
   * @param closure the closure defining nested columns
   * @return this builder for chaining
   */
  public StructColumnBuilder fields(
      @DelegatesTo(value = StructTypeHandler.class, strategy = Closure.DELEGATE_ONLY)
          Closure<?> closure) {
    processNestedClosure(closure);
    return this;
  }

  @Override
  public StructColumnBuilder doc(String doc) {
    super.doc(doc);
    return this;
  }

  private void processNestedClosure(Closure<?> closure) {
    StructTypeHandler nestedHandler = new StructTypeHandler();
    closure.setDelegate(nestedHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();
    this.nestedColumns = nestedHandler.getColumns();
  }

  @Override
  public Column build() {
    return new Column(name, new StructColumnType(new ArrayList<>(nestedColumns)), isOptional, doc);
  }

  /**
   * Get the nested columns.
   *
   * @return list of nested columns
   */
  public List<Column> getNestedColumns() {
    return nestedColumns;
  }
}
