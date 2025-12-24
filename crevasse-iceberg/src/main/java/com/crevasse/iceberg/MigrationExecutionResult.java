package com.crevasse.iceberg;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/**
 * Result of a migration execution operation.
 */
@Getter
@Builder
public class MigrationExecutionResult {

  public enum Status {
    APPLIED,
    SKIPPED_NO_PENDING,
    SKIPPED_NO_SCRIPTS,
    DRY_RUN
  }

  private final Status status;
  private final String database;
  private final String table;
  private final int appliedCount;
  private final int fromStep;
  private final int toStep;
  private final List<String> appliedDescriptions;

  public static MigrationExecutionResult noScripts(String database, String table) {
    return MigrationExecutionResult.builder()
        .status(Status.SKIPPED_NO_SCRIPTS)
        .database(database)
        .table(table)
        .appliedCount(0)
        .build();
  }

  public static MigrationExecutionResult noPending(String database, String table) {
    return MigrationExecutionResult.builder()
        .status(Status.SKIPPED_NO_PENDING)
        .database(database)
        .table(table)
        .appliedCount(0)
        .build();
  }

  public static MigrationExecutionResult applied(
      String database,
      String table,
      int appliedCount,
      int fromStep,
      int toStep,
      List<String> appliedDescriptions) {
    return MigrationExecutionResult.builder()
        .status(Status.APPLIED)
        .database(database)
        .table(table)
        .appliedCount(appliedCount)
        .fromStep(fromStep)
        .toStep(toStep)
        .appliedDescriptions(appliedDescriptions)
        .build();
  }

  public static MigrationExecutionResult dryRun(
      String database,
      String table,
      int appliedCount,
      int fromStep,
      int toStep,
      List<String> appliedDescriptions) {
    return MigrationExecutionResult.builder()
        .status(Status.DRY_RUN)
        .database(database)
        .table(table)
        .appliedCount(appliedCount)
        .fromStep(fromStep)
        .toStep(toStep)
        .appliedDescriptions(appliedDescriptions)
        .build();
  }
}
