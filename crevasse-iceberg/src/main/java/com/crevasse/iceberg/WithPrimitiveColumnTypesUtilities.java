package com.crevasse.iceberg;

import com.crevasse.iceberg.schema.PrimitiveColumnType;
import org.apache.iceberg.types.Types;

public interface WithPrimitiveColumnTypesUtilities {

  default PrimitiveColumnType stringType() {
    return new PrimitiveColumnType(Types.StringType.get());
  }

  default PrimitiveColumnType intType() {
    return new PrimitiveColumnType(Types.IntegerType.get());
  }

  default PrimitiveColumnType longType() {
    return new PrimitiveColumnType(Types.LongType.get());
  }

  default PrimitiveColumnType floatType() {
    return new PrimitiveColumnType(Types.FloatType.get());
  }

  default PrimitiveColumnType doubleType() {
    return new PrimitiveColumnType(Types.DoubleType.get());
  }

  default PrimitiveColumnType booleanType() {
    return new PrimitiveColumnType(Types.BooleanType.get());
  }

  default PrimitiveColumnType dateType() {
    return new PrimitiveColumnType(Types.DateType.get());
  }

  default PrimitiveColumnType timestampType() {
    return new PrimitiveColumnType(Types.TimestampType.withoutZone());
  }

  default PrimitiveColumnType timestampWithZoneType() {
    return new PrimitiveColumnType(Types.TimestampType.withZone());
  }

  default PrimitiveColumnType timeType() {
    return new PrimitiveColumnType(Types.TimeType.get());
  }

  default PrimitiveColumnType decimalType(int precision, int scale) {
    return new PrimitiveColumnType(Types.DecimalType.of(precision, scale));
  }

  default PrimitiveColumnType fixedType(int length) {
    return new PrimitiveColumnType(Types.FixedType.ofLength(length));
  }

  default PrimitiveColumnType binaryType() {
    return new PrimitiveColumnType(Types.BinaryType.get());
  }

  default PrimitiveColumnType uuidType() {
    return new PrimitiveColumnType(Types.UUIDType.get());
  }
}
