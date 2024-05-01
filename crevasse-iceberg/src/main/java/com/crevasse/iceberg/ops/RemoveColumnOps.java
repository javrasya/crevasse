package com.crevasse.iceberg.ops;

import com.crevasse.relocated.com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RemoveColumnOps implements TableOperation {

  private final String columnName;

  public RemoveColumnOps(String columnName) {
    this.columnName = columnName;
  }

  @Override
  public List<String> getImports() {
    return Collections.emptyList();
  }

  @Override
  public String getTemplateName() {
    return "remove_column";
  }

  @Override
  public Map<String, Object> getTemplateVariables() {
    return ImmutableMap.of("columnName", columnName);
  }

  @Override
  public List<String> getDescriptions() {
    return Collections.singletonList("Remove column '" + columnName + "'");
  }

  @Override
  public boolean anythingToRun() {
    return true;
  }
}
