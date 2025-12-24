package com.crevasse.iceberg.schema;

/**
 * Abstract base class for fluent column builders.
 * Provides common functionality for building Column objects with
 * nullable/notNullable and doc() methods.
 */
public abstract class ColumnBuilder {
  protected final String name;
  protected boolean isOptional = true; // Default: nullable
  protected String doc;

  protected ColumnBuilder(String name) {
    this.name = name;
  }

  /**
   * Mark column as nullable (this is the default).
   *
   * @return this builder for chaining
   */
  public ColumnBuilder nullable() {
    this.isOptional = true;
    return this;
  }

  /**
   * Mark column as not nullable (required).
   *
   * @return this builder for chaining
   */
  public ColumnBuilder notNullable() {
    this.isOptional = false;
    return this;
  }

  /**
   * Add documentation to the column.
   *
   * @param doc the documentation string
   * @return this builder for chaining
   */
  public ColumnBuilder doc(String doc) {
    this.doc = doc;
    return this;
  }

  /**
   * Build the final Column object.
   *
   * @return the constructed Column
   */
  public abstract Column build();

  /**
   * Get the column name.
   *
   * @return the column name
   */
  public String getName() {
    return name;
  }

  /**
   * Check if the column is optional (nullable).
   *
   * @return true if optional, false if required
   */
  public boolean isOptional() {
    return isOptional;
  }

  /**
   * Get the documentation string.
   *
   * @return the documentation, or null if not set
   */
  public String getDoc() {
    return doc;
  }
}
