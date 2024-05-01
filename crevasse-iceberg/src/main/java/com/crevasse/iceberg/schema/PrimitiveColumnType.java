package com.crevasse.iceberg.schema;

import com.crevasse.relocated.com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

@Getter
public class PrimitiveColumnType implements ColumnType {
  private final Type.PrimitiveType type;

  public PrimitiveColumnType(Type.PrimitiveType type) {
    Preconditions.checkArgument(type.isPrimitiveType(), "Type must be primitive");
    this.type = type;
  }

  @Override
  public TypeID getTypeId() {
    return type.typeId();
  }

  @Override
  public String code() {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolCol()";
      case INTEGER:
        return "intType()";
      case LONG:
        return "longType()";
      case FLOAT:
        return "floatType()";
      case DOUBLE:
        return "doubleType()";
      case DECIMAL:
        return String.format(
            "decimalType(%s, %s)",
            ((Types.DecimalType) type).precision(), ((Types.DecimalType) type).scale());
      case DATE:
        return "dateType()";
      case TIME:
        return "timeType()";
      case TIMESTAMP:
        if (((Types.TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
          return "timestampWithZoneType()";
        } else {
          return "timestampType()";
        }
      case STRING:
        return "stringType()";
      case UUID:
        return "uuidType()";
      case FIXED:
        return String.format("fixedType(%s)", ((Types.FixedType) type).length());
      case BINARY:
        return "binaryType()";
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
