package com.crevasse.iceberg.helpers;

import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SchemaToStringConverter {

  public static String convert(Types.NestedField nestedField) {
    if (nestedField.doc() == null) {
      return String.format(
          "Types.NestedField.of(%d, %s, '%s', %s)",
          nestedField.fieldId(),
          nestedField.isOptional(),
          nestedField.name(),
          typeName(nestedField.type()));
    } else {
      return String.format(
          "Types.NestedField.of(%d, %s, %s, '%s', %s)",
          nestedField.fieldId(),
          nestedField.isOptional(),
          nestedField.name(),
          typeName(nestedField.type()),
          nestedField.doc());
    }
  }

  public static String typeName(Type type) {
    switch (type.typeId()) {
      case STRUCT:
        return "Types.StructType.of(\n\t\t"
            + type.asStructType().fields().stream()
                .map(SchemaToStringConverter::convert)
                .collect(Collectors.joining(",\n\t\t"))
            + "\n\t)";
      case LIST:
        if (type.asListType().isElementRequired()) {
          return "Types.ListType.ofRequired(\n\t\t"
              + type.asListType().elementId()
              + ",\n\t\t"
              + typeName(type.asListType().elementType())
              + "\n\t)";
        } else {
          return "Types.ListType.ofOptional(\n\t\t"
              + type.asListType().elementId()
              + ",\n\t\t"
              + typeName(type.asListType().elementType())
              + "\n\t)";
        }
      case MAP:
        if (type.asMapType().isValueRequired()) {
          return "Types.MapType.ofRequired(\n\t\t"
              + type.asMapType().keyId()
              + ",\n\t\t"
              + type.asMapType().valueId()
              + ",\n\t\t"
              + typeName(type.asMapType().keyType())
              + ",\n\t\t"
              + typeName(type.asMapType().valueType())
              + "\n\t)";
        } else {
          return "Types.MapType.ofOptional(\n\t\t"
              + type.asMapType().keyId()
              + ",\n\t\t"
              + type.asMapType().valueId()
              + ",\n\t\t"
              + typeName(type.asMapType().keyType())
              + ",\n\t\t"
              + typeName(type.asMapType().valueType())
              + "\n\t)";
        }
      case BOOLEAN:
        return "Types.BooleanType.get()";
      case INTEGER:
        return "Types.IntegerType.get()";
      case LONG:
        return "Types.LongType.get()";
      case FLOAT:
        return "Types.FloatType.get()";
      case DOUBLE:
        return "Types.DoubleType.get()";
      case DATE:
        return "Types.DateType.get()";
      case TIME:
        return "Types.TimeType.get()";
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return "Types.TimestampType.withZone()";
        } else {
          return "Types.TimestampType.withoutZone()";
        }
      case STRING:
        return "Types.StringType.get()";
      case UUID:
        return "Types.UUIDType.get()";
      case FIXED:
        return "Types.FixedType.ofLength(" + ((Types.FixedType) type).length() + ")";
      case BINARY:
        return "Types.BinaryType.get()";
      case DECIMAL:
        return "Types.DecimalType.of("
            + ((Types.DecimalType) type).precision()
            + ", "
            + ((Types.DecimalType) type).scale()
            + ")";
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.typeId());
    }
  }

  public static String typeNameOnly(Type type) {
    switch (type.typeId()) {
      case STRUCT:
        return "Types.StructType.of(\n\t\t"
            + type.asStructType().fields().stream()
                .map(SchemaToStringConverter::convert)
                .collect(Collectors.joining(",\n\t\t"))
            + "\n\t)";
      case LIST:
        if (type.asListType().isElementRequired()) {
          return "Types.ListType.ofRequired(\n\t\t"
              + type.asListType().elementId()
              + ",\n\t\t"
              + typeName(type.asListType().elementType())
              + "\n\t)";
        } else {
          return "Types.ListType.ofOptional(\n\t\t"
              + type.asListType().elementId()
              + ",\n\t\t"
              + typeName(type.asListType().elementType())
              + "\n\t)";
        }
      case MAP:
        if (type.asMapType().isValueRequired()) {
          return "Types.MapType.ofRequired(\n\t\t"
              + type.asMapType().keyId()
              + ",\n\t\t"
              + type.asMapType().valueId()
              + ",\n\t\t"
              + typeName(type.asMapType().keyType())
              + ",\n\t\t"
              + typeName(type.asMapType().valueType())
              + "\n\t)";
        } else {
          return "Types.MapType.ofOptional(\n\t\t"
              + type.asMapType().keyId()
              + ",\n\t\t"
              + type.asMapType().valueId()
              + ",\n\t\t"
              + typeName(type.asMapType().keyType())
              + ",\n\t\t"
              + typeName(type.asMapType().valueType())
              + "\n\t)";
        }
      case BOOLEAN:
        return "Types.BooleanType.get()";
      case INTEGER:
        return "Types.IntegerType.get()";
      case LONG:
        return "Types.LongType.get()";
      case FLOAT:
        return "Types.FloatType.get()";
      case DOUBLE:
        return "Types.DoubleType.get()";
      case DATE:
        return "Types.DateType.get()";
      case TIME:
        return "Types.TimeType.get()";
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return "Types.TimestampType.withZone()";
        } else {
          return "Types.TimestampType.withoutZone()";
        }
      case STRING:
        return "Types.StringType.get()";
      case UUID:
        return "Types.UUIDType.get()";
      case FIXED:
        return "Types.FixedType.ofLength(" + ((Types.FixedType) type).length() + ")";
      case BINARY:
        return "Types.BinaryType.get()";
      case DECIMAL:
        return "Types.DecimalType.of("
            + ((Types.DecimalType) type).precision()
            + ", "
            + ((Types.DecimalType) type).scale()
            + ")";
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.typeId());
    }
  }
}
