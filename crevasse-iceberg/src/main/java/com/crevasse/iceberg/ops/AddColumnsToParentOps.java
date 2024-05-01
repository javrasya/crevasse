package com.crevasse.iceberg.ops;

import com.crevasse.iceberg.schema.Column;
import com.crevasse.relocated.com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AddColumnsToParentOps implements TableOperation {

  private final String parent;
  private final List<Column> columns;

  public AddColumnsToParentOps(String parent, List<Column> columns) {
    this.parent = parent;
    this.columns = columns;
  }

  @Override
  public List<String> getImports() {
    return Collections.emptyList();
  }

  @Override
  public String getTemplateName() {
    return "add_columns_to_parent";
  }

  @Override
  public Map<String, Object> getTemplateVariables() {
    return ImmutableMap.of(
        "parent", parent,
        "columns", columns);
  }

  @Override
  public List<String> getDescriptions() {
    return columns.stream()
        .map(column -> "Add column '" + column.getName() + "' to parent '" + parent + "'")
        .collect(Collectors.toList());
  }

  @Override
  public boolean anythingToRun() {
    return !columns.isEmpty();
  }
}
