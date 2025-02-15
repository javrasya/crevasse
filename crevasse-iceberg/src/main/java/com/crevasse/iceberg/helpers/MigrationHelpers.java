package com.crevasse.iceberg.helpers;

import com.crevasse.iceberg.MigrationContext;
import com.crevasse.iceberg.MigrationScriptContainer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class MigrationHelpers {

  public static MigrationContext getContextWithFakeTable() throws IOException {
    return new MigrationContext(getFakeIcebergCatalog(), TableIdentifier.of("fakeTable"));
  }

  public static org.apache.iceberg.catalog.Catalog getFakeIcebergCatalog() {
    try {
      final Path fakeIcebergWarehouse = Files.createTempDirectory("fake_iceberg_warehouse_");
      return new HadoopCatalog(
          new Configuration(), "file://" + fakeIcebergWarehouse.toAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<MigrationScriptContainer> scanExistingMigrationScripts(
          String pathToMigrationScripts, TableIdentifier tableIdentifier) throws IOException {
    final Path migrationScriptFolder =
        Paths.get(
            pathToMigrationScripts, tableIdentifier.namespace() + "_" + tableIdentifier.name());

    if (!Files.exists(migrationScriptFolder)) {
      migrationScriptFolder.toFile().mkdirs();
    }

    try (Stream<Path> paths = Files.list(migrationScriptFolder)) {
      return paths
          .filter(
              f ->
                  f.getFileName().toString().startsWith("migration_")
                      && f.getFileName().toString().endsWith(".groovy"))
          .map(Path::toAbsolutePath)
          .map(MigrationScriptContainer::fromPath)
          .collect(Collectors.toList());
    }
  }
}
