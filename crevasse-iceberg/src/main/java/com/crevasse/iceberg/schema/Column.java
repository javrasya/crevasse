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

  /**
   * Generate the DSL code for this column using the fluent builder pattern.
   *
   * @return the DSL code string
   */
  public String code() {
    StringBuilder sb = new StringBuilder();

    // Generate the column method call
    switch (columnType.getTypeId()) {
      case STRING:
        sb.append(String.format("stringCol('%s')", name));
        break;
      case INTEGER:
        sb.append(String.format("intCol('%s')", name));
        break;
      case LONG:
        sb.append(String.format("longCol('%s')", name));
        break;
      case FLOAT:
        sb.append(String.format("floatCol('%s')", name));
        break;
      case DOUBLE:
        sb.append(String.format("doubleCol('%s')", name));
        break;
      case BOOLEAN:
        sb.append(String.format("boolCol('%s')", name));
        break;
      case DATE:
        sb.append(String.format("dateCol('%s')", name));
        break;
      case TIME:
        sb.append(String.format("timeCol('%s')", name));
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) getType()).shouldAdjustToUTC()) {
          sb.append(String.format("timestampWithZoneCol('%s')", name));
        } else {
          sb.append(String.format("timestampCol('%s')", name));
        }
        break;
      case UUID:
        sb.append(String.format("uuidCol('%s')", name));
        break;
      case BINARY:
        sb.append(String.format("binaryCol('%s')", name));
        break;
      case DECIMAL:
        sb.append(
            String.format(
                "decimalCol('%s', %d, %d)",
                name,
                ((Types.DecimalType) getType()).precision(),
                ((Types.DecimalType) getType()).scale()));
        break;
      case FIXED:
        sb.append(
            String.format(
                "fixedCol('%s', %d)", name, ((Types.FixedType) getType()).length()));
        break;
      case LIST:
        sb.append(
            String.format(
                "listCol('%s', %s)",
                name, ((ListColumnType) columnType).getElementType().code()));
        break;
      case MAP:
        MapColumnType mapType = (MapColumnType) columnType;
        sb.append(
            String.format(
                "mapCol('%s', %s, %s)",
                name, mapType.getKeyType().code(), mapType.getValueType().code()));
        break;
      case STRUCT:
        sb.append(String.format("structCol('%s')", name));
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + columnType.getTypeId());
    }

    // Handle struct types with closures
    if (columnType.getTypeId() == Type.TypeID.STRUCT) {
      appendStructNullabilityAndContent(sb);
    } else {
      // For primitive, list, and map types, add fluent methods
      appendNullabilityAndDoc(sb);
    }

    return sb.toString();
  }

  private void appendNullabilityAndDoc(StringBuilder sb) {
    // Only add .notNullable() when required (nullable is the default)
    if (!isOptional) {
      sb.append(".notNullable()");
    }
    // Add .doc() if present
    if (doc != null) {
      sb.append(String.format(".doc('%s')", escapeString(doc)));
    }
  }

  private void appendStructNullabilityAndContent(StringBuilder sb) {
    StructColumnType structType = (StructColumnType) columnType;
    String nestedContent =
        structType.getColumns().stream()
            .map(Column::code)
            .collect(Collectors.joining("\n\t\t"));

    // Use nullability method with closure
    if (!isOptional) {
      sb.append(".notNullable {\n\t\t");
    } else {
      sb.append(".nullable {\n\t\t");
    }
    sb.append(nestedContent);
    sb.append("\n\t}");

    // Add .doc() if present (after the closure)
    if (doc != null) {
      sb.append(String.format(".doc('%s')", escapeString(doc)));
    }
  }

  private String escapeString(String s) {
    return s.replace("\\", "\\\\").replace("'", "\\'");
  }

  public Type getType() {
    return getType(columnType);
  }

  private Type getType(ColumnType columnType) {
    if (columnType instanceof PrimitiveColumnType) {
      return ((PrimitiveColumnType) columnType).getType();
    } else {
      switch (columnType.getTypeId()) {
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
}
