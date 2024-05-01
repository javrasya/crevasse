package com.crevasse.iceberg;

import static groovy.lang.Closure.DELEGATE_ONLY;

import com.crevasse.iceberg.schema.ColumnType;
import com.crevasse.iceberg.schema.ListColumnType;
import com.crevasse.iceberg.schema.MapColumnType;
import com.crevasse.iceberg.schema.StructColumnType;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;

public interface WithColumnTypesUtilities extends WithPrimitiveColumnTypesUtilities {

  default ColumnType listType(ColumnType elementType) {
    return new ListColumnType(elementType);
  }

  default ColumnType mapType(ColumnType keyType, ColumnType valueType) {
    return new MapColumnType(keyType, valueType);
  }

  default ColumnType structType(@DelegatesTo(StructTypeHandler.class) Closure closure) {
    StructTypeHandler structTypeHandler = new StructTypeHandler();
    closure.setDelegate(structTypeHandler);
    closure.setResolveStrategy(DELEGATE_ONLY);
    closure.call();

    return new StructColumnType(structTypeHandler.getColumns());
  }
}
