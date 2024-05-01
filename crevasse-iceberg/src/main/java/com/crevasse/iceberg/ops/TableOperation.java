package com.crevasse.iceberg.ops;

import java.util.List;
import java.util.Map;

public interface TableOperation {

  List<String> getImports();

  String getTemplateName();

  Map<String, Object> getTemplateVariables();

  List<String> getDescriptions();

  boolean anythingToRun();
}
