package com.crevasse.iceberg.ops;

import com.crevasse.iceberg.schema.ColumnType;
import com.crevasse.relocated.com.google.common.collect.ImmutableMap;
import java.util.*;

public class ModifyColumnOps implements TableOperation {

  private final Set<String> columnsMarkedAsRequired;
  private final Set<String> columnsMarkedAsOptional;
  private final Map<String, ColumnType> updatedColumnTypes;

  public ModifyColumnOps(
      Set<String> columnsMarkedAsRequired,
      Set<String> columnsMarkedAsOptional,
      Map<String, ColumnType> updatedColumnTypes) {
    this.columnsMarkedAsRequired = columnsMarkedAsRequired;
    this.columnsMarkedAsOptional = columnsMarkedAsOptional;
    this.updatedColumnTypes = updatedColumnTypes;
  }

  @Override
  public List<String> getImports() {
    return Collections.emptyList();
  }

  @Override
  public String getTemplateName() {
    return "modify_columns";
  }

  @Override
  public Map<String, Object> getTemplateVariables() {
    return ImmutableMap.of("modifyColumnOps", getModifyColumnOps());
  }

  private final List<String> getModifyColumnOps() {
    final List<String> modifyColumnOps = new ArrayList<>();
    columnsMarkedAsRequired.forEach(
        columnName -> modifyColumnOps.add("requireColumn('" + columnName + "')"));
    columnsMarkedAsOptional.forEach(
        columnName -> modifyColumnOps.add("makeColumnOptional('" + columnName + "')"));
    updatedColumnTypes.forEach(
        (columnName, columnType) ->
            modifyColumnOps.add(
                "updateColumnType('" + columnName + "', " + columnType.code() + ")"));
    return modifyColumnOps;
  }

  @Override
  public List<String> getDescriptions() {
    final List<String> descriptions = new ArrayList<>();
    columnsMarkedAsRequired.forEach(
        columnName -> descriptions.add("Require column '" + columnName + "'"));
    columnsMarkedAsOptional.forEach(
        columnName -> descriptions.add("Mark column '" + columnName + "' as optional"));
    updatedColumnTypes.forEach(
        (columnName, columnType) ->
            descriptions.add(
                "Update column type of '" + columnName + "' to " + columnType.getTypeId()));
    return descriptions;
  }

  @Override
  public boolean anythingToRun() {
    return true;
  }
}
