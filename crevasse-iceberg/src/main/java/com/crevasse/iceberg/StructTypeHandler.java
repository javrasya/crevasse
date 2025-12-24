package com.crevasse.iceberg;

import com.crevasse.iceberg.schema.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.types.Types;

/**
 * Handler for struct type definitions in the migration DSL.
 * Provides fluent column builder methods that return ColumnBuilder instances.
 */
@Getter
@ToString
public class StructTypeHandler implements WithColumnTypesUtilities {
  private final List<ColumnBuilder> columnBuilders = new ArrayList<>();

  /**
   * Get the built columns. This finalizes all builders.
   *
   * @return list of built Column objects
   */
  public List<Column> getColumns() {
    return columnBuilders.stream().map(ColumnBuilder::build).collect(Collectors.toList());
  }

  // === Primitive column methods - return builders ===

  public PrimitiveColumnBuilder stringCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.StringType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder intCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.IntegerType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder longCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.LongType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder floatCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.FloatType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder doubleCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.DoubleType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder boolCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.BooleanType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder dateCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.DateType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder timestampCol(String name) {
    PrimitiveColumnBuilder builder =
        new PrimitiveColumnBuilder(name, Types.TimestampType.withoutZone());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder timestampWithZoneCol(String name) {
    PrimitiveColumnBuilder builder =
        new PrimitiveColumnBuilder(name, Types.TimestampType.withZone());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder timeCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.TimeType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder binaryCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.BinaryType.get());
    columnBuilders.add(builder);
    return builder;
  }

  public PrimitiveColumnBuilder uuidCol(String name) {
    PrimitiveColumnBuilder builder = new PrimitiveColumnBuilder(name, Types.UUIDType.get());
    columnBuilders.add(builder);
    return builder;
  }

  // === Parameterized primitive types ===

  public DecimalColumnBuilder decimalCol(String name, int precision, int scale) {
    DecimalColumnBuilder builder = new DecimalColumnBuilder(name, precision, scale);
    columnBuilders.add(builder);
    return builder;
  }

  public FixedColumnBuilder fixedCol(String name, int length) {
    FixedColumnBuilder builder = new FixedColumnBuilder(name, length);
    columnBuilders.add(builder);
    return builder;
  }

  // === Complex types ===

  public ListColumnBuilder listCol(String name, ColumnType elementType) {
    ListColumnBuilder builder = new ListColumnBuilder(name, elementType);
    columnBuilders.add(builder);
    return builder;
  }

  public MapColumnBuilder mapCol(String name, ColumnType keyType, ColumnType valueType) {
    MapColumnBuilder builder = new MapColumnBuilder(name, keyType, valueType);
    columnBuilders.add(builder);
    return builder;
  }

  public StructColumnBuilder structCol(String name) {
    StructColumnBuilder builder = new StructColumnBuilder(name);
    columnBuilders.add(builder);
    return builder;
  }

}
