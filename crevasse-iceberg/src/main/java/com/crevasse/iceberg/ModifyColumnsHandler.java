package com.crevasse.iceberg;

import com.crevasse.iceberg.schema.PrimitiveColumnType;
import com.crevasse.relocated.com.google.common.collect.Streams;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
class ModifyColumnsHandler implements WithPrimitiveColumnTypesUtilities {
  private final Set<String> columnsMarkedAsRequired = new HashSet<>();
  private final Set<String> columnsMarkedAsOptional = new HashSet<>();
  private final Map<String, PrimitiveColumnType> updatedColumnTypes = new HashMap<>();

  public void requireColumn(String columnName) {
    columnsMarkedAsRequired.add(columnName);
    columnsMarkedAsOptional.remove(columnName);
  }

  public void makeColumnOptional(String columnName) {
    columnsMarkedAsOptional.add(columnName);
    columnsMarkedAsRequired.remove(columnName);
  }

  public void updateColumnType(String columnName, PrimitiveColumnType columnType) {
    updatedColumnTypes.put(columnName, columnType);
  }

  Set<String> getAllUpdatedColumns() {
    return Streams.concat(
            columnsMarkedAsRequired.stream(),
            columnsMarkedAsOptional.stream(),
            updatedColumnTypes.keySet().stream())
        .collect(Collectors.toSet());
  }
}
