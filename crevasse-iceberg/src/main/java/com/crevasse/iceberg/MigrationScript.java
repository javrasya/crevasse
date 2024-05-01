package com.crevasse.iceberg;

import com.crevasse.iceberg.ops.TableOperation;
import com.crevasse.relocated.com.google.common.collect.ImmutableMap;
import com.hubspot.jinjava.Jinjava;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
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
public class MigrationScript implements Serializable {
  private final String code;

  public MigrationScript(String code) {
    this.code = code;
  }

  public static MigrationScript fromPath(Path path) {
    try {
      final String code = getScript(path);
      return new MigrationScript(code);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static MigrationScript fromResource(String resource) {
    try {
      final String code = readResource(resource);
      return new MigrationScript(code);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Path generateAndGetPath(
      int stepNumber, List<TableOperation> tableOperations, Path directory) throws IOException {
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

    final String script =
        jinjava.render(
            readResource("/templates/migration_script.groovy.j2"),
            ImmutableMap.of(
                "ops",
                opsBuilder.toString(),
                "step",
                stepNumber,
                "imports",
                imports.stream().sorted().collect(Collectors.toList()),
                "descriptions",
                stepNumber == 0
                    ? Collections.singletonList("Initial schema creation")
                    : descriptions));
    Path scriptPath = Paths.get(directory.toString(), "migration_" + stepNumber + ".groovy");
    Files.write(scriptPath, script.getBytes());
    return scriptPath;
  }

  void run(MigrationStep migrationStep) throws IOException {
    final Binding binding = new Binding();
    binding.setVariable("step", migrationStep);
    final GroovyShell shell = new GroovyShell(binding);

    shell.evaluate(code);
  }

  private static String getScript(Path path) throws IOException {
    return new String(Files.readAllBytes(path));
  }

  private static String readResource(String resource) throws IOException {
    try (InputStream in = MigrationScript.class.getResourceAsStream(resource)) {
      return new BufferedReader(
              new InputStreamReader(Objects.requireNonNull(in), StandardCharsets.UTF_8))
          .lines()
          .collect(Collectors.joining("\n"));
    }
  }
}
