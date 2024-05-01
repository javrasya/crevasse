package com.crevasse.iceberg.schema;

import org.apache.iceberg.types.Type;

public interface ColumnType {
  Type.TypeID getTypeId();

  String code();
}
