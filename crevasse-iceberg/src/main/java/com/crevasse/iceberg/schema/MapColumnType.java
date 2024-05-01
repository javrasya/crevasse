package com.crevasse.iceberg.schema;

import lombok.Getter;
import org.apache.iceberg.types.Type;

@Getter
public class MapColumnType implements ColumnType {
  private final ColumnType keyType;
  private final ColumnType valueType;

  public MapColumnType(ColumnType keyType, ColumnType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public Type.TypeID getTypeId() {
    return Type.TypeID.MAP;
  }

  @Override
  public String code() {
    return String.format("mapType(%s, %s)", keyType.code(), valueType.code());
  }
}
