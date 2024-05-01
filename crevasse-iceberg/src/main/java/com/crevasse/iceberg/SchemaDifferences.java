package com.crevasse.iceberg;

import com.crevasse.iceberg.ops.*;
import com.crevasse.iceberg.schema.Column;
import com.crevasse.iceberg.schema.ColumnType;
import java.io.Serializable;
import java.util.*;
import lombok.Getter;

@Getter
public class SchemaDifferences implements Serializable {

  private final List<Column> addedColumns = new ArrayList<>();
  private final Set<String> removedColumns = new HashSet<>();
  private final Set<String> columnsMarkedAsRequired = new HashSet<>();
  private final Set<String> columnsMarkedAsOptional = new HashSet<>();
  private final Map<String, ColumnType> updatedColumnTypes = new HashMap<>();
  private final List<ModifiedField> modifiedColumns = new ArrayList<>();
  private final Map<String, List<Column>> addedColumnsToParent = new HashMap<>();

  public void addField(Column column) {
    addedColumns.add(column);
  }

  public void removeField(String columnName) {
    removedColumns.add(columnName);
  }

  public void requireColumn(String columnName) {
    columnsMarkedAsRequired.add(columnName);
  }

  public void markColumnAsOptional(String columnName) {
    columnsMarkedAsOptional.add(columnName);
  }

  public void updateColumnType(String columnName, ColumnType columnType) {
    updatedColumnTypes.put(columnName, columnType);
  }

  public void addFieldToParent(String parentName, Column column) {
    List<Column> columns = addedColumnsToParent.getOrDefault(parentName, new ArrayList<>());
    columns.add(column);
    addedColumnsToParent.put(parentName, columns);
  }

  @Getter
  public static class ModifiedField implements Serializable {
    private final Column oldColumn;
    private final Column newColumn;

    public ModifiedField(Column oldColumn, Column newColumn) {
      this.oldColumn = oldColumn;
      this.newColumn = newColumn;
    }
  }

  public List<TableOperation> getTableOperations() {
    List<TableOperation> tableOperations = new ArrayList<>();
    if (!addedColumns.isEmpty()) {
      tableOperations.add(new AddColumnsOps(addedColumns));
    }
    removedColumns.forEach(columnName -> tableOperations.add(new RemoveColumnOps(columnName)));

    if (!columnsMarkedAsRequired.isEmpty()
        || !columnsMarkedAsOptional.isEmpty()
        || !updatedColumnTypes.isEmpty()) {
      tableOperations.add(
          new ModifyColumnOps(
              columnsMarkedAsRequired, columnsMarkedAsOptional, updatedColumnTypes));
    }

    addedColumnsToParent.forEach(
        (fieldNameWithParent, columns) ->
            tableOperations.add(
                new AddColumnsToParentOps(fieldNameWithParent.split("\\.")[0], columns)));
    return tableOperations;
  }
}
