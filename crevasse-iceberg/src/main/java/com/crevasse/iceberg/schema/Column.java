package com.crevasse.iceberg.schema;

import groovy.transform.PackageScope;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

@Getter
public class Column {
  @PackageScope final String name;
  @PackageScope final ColumnType columnType;
  @PackageScope final boolean isOptional;
  @PackageScope final String doc;

  public Column(String name, ColumnType columnType, boolean isOptional, String doc) {
    this.name = name;
    this.columnType = columnType;
    this.isOptional = isOptional;
    this.doc = doc;
  }

  public boolean isRequired() {
    return !isOptional;
  }

  public String code() {
    switch (columnType.getTypeId()) {
      case BOOLEAN:
        if (this.doc == null) return String.format("boolCol('%s', %s)", name, isOptional);
        else return String.format("boolCol('%s', %s, '%s')", name, isOptional, doc);
      case INTEGER:
        if (this.doc == null) return String.format("intCol('%s', %s)", name, isOptional);
        else return String.format("intCol('%s', %s, '%s')", name, isOptional, doc);
      case LONG:
        if (this.doc == null) return String.format("longCol('%s', %s)", name, isOptional);
        else return String.format("longCol('%s', %s, '%s')", name, isOptional, doc);
      case FLOAT:
        if (this.doc == null) return String.format("floatCol('%s', %s)", name, isOptional);
        else return String.format("floatCol('%s', %s, '%s')", name, isOptional, doc);
      case DOUBLE:
        if (this.doc == null) return String.format("doubleCol('%s', %s)", name, isOptional);
        else return String.format("doubleCol('%s', %s, '%s')", name, isOptional, doc);
      case DECIMAL:
        if (this.doc == null)
          return String.format(
              "decimalCol('%s', %s, %s, %s)",
              name,
              isOptional,
              ((Types.DecimalType) getType()).precision(),
              ((Types.DecimalType) getType()).scale());
        else
          return String.format(
              "decimalCol('%s', %s, %s, %s, '%s')",
              name,
              isOptional,
              ((Types.DecimalType) getType()).precision(),
              ((Types.DecimalType) getType()).scale(),
              doc);
      case DATE:
        if (this.doc == null) return String.format("dateCol('%s', %s)", name, isOptional);
        else return String.format("dateCol('%s', %s, '%s')", name, isOptional, doc);
      case TIME:
        if (this.doc == null) return String.format("timeCol('%s', %s)", name, isOptional);
        else return String.format("timeCol('%s', %s, '%s')", name, isOptional, doc);
      case TIMESTAMP:
        if (((Types.TimestampType) getType()).shouldAdjustToUTC()) {
          if (this.doc == null)
            return String.format("timestampWithZoneCol('%s', %s)", name, isOptional);
          else return String.format("timestampWithZoneCol('%s', %s, '%s')", name, isOptional, doc);
        } else {
          if (this.doc == null) return String.format("timestampCol('%s', %s)", name, isOptional);
          else return String.format("timestampCol('%s', %s, '%s')", name, isOptional, doc);
        }
      case STRING:
        if (this.doc == null) return String.format("stringCol('%s', %s)", name, isOptional);
        else return String.format("stringCol('%s', %s, '%s')", name, isOptional, doc);
      case UUID:
        if (this.doc == null) return String.format("uuidCol('%s', %s)", name, isOptional);
        else return String.format("uuidCol('%s', %s, '%s')", name, isOptional, doc);
      case FIXED:
        if (this.doc == null)
          return String.format(
              "fixedCol('%s', %s, %s)", name, isOptional, ((Types.FixedType) getType()).length());
        else
          return String.format(
              "fixedCol('%s', %s, %s, '%s')",
              name, isOptional, ((Types.FixedType) getType()).length(), doc);
      case BINARY:
        if (this.doc == null) return String.format("binaryCol('%s', %s)", name, isOptional);
        else return String.format("binaryCol('%s', %s, '%s')", name, isOptional, doc);

      case MAP:
        if (this.doc == null)
          return String.format(
              "mapCol('%s', %s) {\n\t" + "key %s\n\t\t" + "value %s\n\t\t" + "\n\t}",
              name,
              isOptional,
              ((MapColumnType) columnType).getKeyType().code(),
              ((MapColumnType) columnType).getValueType().code());
        else
          return String.format(
              "mapCol('%s', %s, '%s') {\n\t" + "key %s\n\t\t" + "value %s\n\t\t" + "\n\t}",
              name,
              isOptional,
              doc,
              ((MapColumnType) columnType).getKeyType().code(),
              ((MapColumnType) columnType).getValueType().code());
      case LIST:
        if (this.doc == null)
          return String.format(
              "listCol('%s', %s, %s)",
              name, isOptional, ((ListColumnType) columnType).getElementType().code());
        else
          return String.format(
              "listCol('%s', %s,  '%s', %s)",
              name, isOptional, doc, ((ListColumnType) columnType).getElementType().code());

      case STRUCT:
        if (this.doc == null)
          return String.format(
              "structCol('%s', %s) {\n\t" + "%s\n\t}",
              name,
              isOptional,
              ((StructColumnType) columnType)
                  .getColumns().stream().map(Column::code).collect(Collectors.joining("\n\t\t")));
        else
          return String.format(
              "structCol('%s', %s, '%s') {\n\t" + "%s\n\t}",
              name,
              isOptional,
              doc,
              ((StructColumnType) columnType)
                  .getColumns().stream().map(Column::code).collect(Collectors.joining("\n\t\t")));
      default:
        throw new IllegalArgumentException("Unsupported type: " + columnType.getTypeId());
    }
  }

  public Type getType() {
    return getType(columnType);
  }

  private Type getType(ColumnType columnType) {
    switch (columnType.getTypeId()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INTEGER:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
        return Types.DecimalType.of(0, 0);
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case STRING:
        return Types.StringType.get();
      case UUID:
        return Types.UUIDType.get();
      case FIXED:
        return Types.FixedType.ofLength(0);
      case BINARY:
        return Types.BinaryType.get();
      case STRUCT:
        final List<Types.NestedField> fields =
            ((StructColumnType) columnType)
                .getColumns().stream()
                    .map(
                        column -> {
                          if (column.isOptional()) {
                            return Types.NestedField.optional(
                                0, column.getName(), column.getType(), column.getDoc());
                          } else {
                            return Types.NestedField.required(
                                0, column.getName(), column.getType(), column.getDoc());
                          }
                        })
                    .collect(Collectors.toList());

        return Types.StructType.of(fields);
      case MAP:
        final MapColumnType mapColumnType = (MapColumnType) columnType;
        return Types.MapType.ofOptional(
            0, 0, getType(mapColumnType.getKeyType()), getType(mapColumnType.getValueType()));
      case LIST:
        final ListColumnType listColumnType = (ListColumnType) columnType;
        return Types.ListType.ofOptional(0, getType(listColumnType.getElementType()));
      default:
        throw new IllegalArgumentException("Unsupported type: " + columnType.getTypeId());
    }
  }
}
