package com.crevasse.iceberg;

import com.crevasse.iceberg.ops.TableOperation;
import com.crevasse.relocated.com.google.common.base.Preconditions;
import com.hubspot.jinjava.Jinjava;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.transform.BaseScript;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class MigrationScriptContainer implements Serializable {
  private final MigrationBaseScript script;

  private MigrationScriptContainer(MigrationBaseScript script) {
    this.script = script;
  }

  public static MigrationScriptContainer fromScript(MigrationBaseScript script) {
    return new MigrationScriptContainer(script);
  }

  public static MigrationScriptContainer fromPath(Path path) {
    try {
      final String code = getScriptCode(path);

      return new MigrationScriptContainer(getScript(code));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static MigrationScriptContainer fromResource(String resource, ClassLoader classLoader) {
    try {
      final String code = readResource(resource);
      return new MigrationScriptContainer(getScript(code));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Path generateAndGetPath(
      String scriptDir,
      int stepNumber,
      List<TableOperation> tableOperations,
      String database,
      String table)
      throws IOException {
    final Jinjava jinjava = new Jinjava();
    final StringBuilder opsBuilder = new StringBuilder();
    final Set<String> imports = new HashSet<>();
    final List<String> descriptions = new ArrayList<>();
    for (TableOperation tableOperation : tableOperations) {
      if (tableOperation.anythingToRun()) {
        final String templateName = tableOperation.getTemplateName();
        imports.addAll(tableOperation.getImports());
        final Map<String, Object> templateVariables = tableOperation.getTemplateVariables();
        final String renderedOps =
            jinjava.render(readResource("/templates/" + templateName + ".j2"), templateVariables);
        opsBuilder.append(renderedOps).append("\n");

        descriptions.addAll(tableOperation.getDescriptions());
      }
    }

    imports.add(MigrationBaseScript.class.getName());
    imports.add(BaseScript.class.getName());
    Map<String, Object> bindings = new HashMap<>();
    bindings.put("ops", opsBuilder.toString());
    bindings.put("step", stepNumber);
    bindings.put("imports", imports.stream().sorted().collect(Collectors.toList()));
    bindings.put(
        "descriptions",
        stepNumber == 0 ? Collections.singletonList("Initial schema creation") : descriptions);
    bindings.put("database", database);
    bindings.put("table", table);

    final String script =
        jinjava.render(readResource("/templates/migration_script.groovy.j2"), bindings);
    final Path directoryPath = Paths.get(scriptDir, database, table);
    Path scriptPath = Paths.get(directoryPath.toString(), "migration_" + stepNumber + ".groovy");
    Files.write(scriptPath, script.getBytes());
    return scriptPath;
  }

  void run(MigrationStep migrationStep) {
    final Binding binding = new Binding();
    binding.setVariable("step", migrationStep);
    script.setBinding(binding);
    script.execute();
  }

  private static MigrationBaseScript getScript(String code) {
    final Script script =
        new GroovyShell(MigrationScriptContainer.class.getClassLoader()).parse(code);
    Preconditions.checkArgument(
        script instanceof MigrationBaseScript, "Script must extend MigrationBaseScript");
    script.run();
    return (MigrationBaseScript) script;
  }

  private static String getScriptCode(Path path) throws IOException {
    return new String(Files.readAllBytes(path));
  }

  private static String readResource(String resource) throws IOException {
    try (InputStream in = MigrationScriptContainer.class.getResourceAsStream(resource)) {
      return new BufferedReader(
              new InputStreamReader(Objects.requireNonNull(in), StandardCharsets.UTF_8))
          .lines()
          .collect(Collectors.joining("\n"));
    }
  }
}
