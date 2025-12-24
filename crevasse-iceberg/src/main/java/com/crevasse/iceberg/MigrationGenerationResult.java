package com.crevasse.iceberg;

import java.nio.file.Path;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Result of a migration script generation operation.
 */
@Getter
@Builder
public class MigrationGenerationResult {

  public enum Status {
    CREATED,
    SKIPPED_NO_CHANGES
  }

  private final Status status;
  private final String database;
  private final String table;
  private final Path scriptPath;
  private final int stepNumber;
  private final List<String> operations;

  public static MigrationGenerationResult noChanges(String database, String table) {
    return MigrationGenerationResult.builder()
        .status(Status.SKIPPED_NO_CHANGES)
        .database(database)
        .table(table)
        .build();
  }

  public static MigrationGenerationResult created(
      String database,
      String table,
      Path scriptPath,
      int stepNumber,
      List<String> operations) {
    return MigrationGenerationResult.builder()
        .status(Status.CREATED)
        .database(database)
        .table(table)
        .scriptPath(scriptPath)
        .stepNumber(stepNumber)
        .operations(operations)
        .build();
  }
}
