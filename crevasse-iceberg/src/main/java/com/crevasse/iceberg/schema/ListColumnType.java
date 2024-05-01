package com.crevasse.iceberg.schema;

import lombok.Getter;
import org.apache.iceberg.types.Type;

@Getter
public class ListColumnType implements ColumnType {
  private final ColumnType elementType;

  public ListColumnType(ColumnType elementType) {
    this.elementType = elementType;
  }

  @Override
  public Type.TypeID getTypeId() {
    return Type.TypeID.LIST;
  }

  @Override
  public String code() {
    return String.format("listType(%s)", elementType.code());
  }
}
