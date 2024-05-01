package com.crevasse.iceberg;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.expressions.UnboundTerm;

class PartitionColumnHandler {
  private Map<String, PartitionColumn> partitionColumns = new HashMap<>();

  public void ref(String name) {
    addPartitionColumn(name, "", Expressions.ref(name));
  }

  public void bucket(String name, int numBuckets) {
    addPartitionColumn(name, "_bucket", Expressions.bucket(name, numBuckets));
  }

  public void day(String name) {
    addPartitionColumn(name, "_day", Expressions.day(name));
  }

  public void hour(String name) {
    addPartitionColumn(name, "_hour", Expressions.hour(name));
  }

  public void month(String name) {
    addPartitionColumn(name, "_month", Expressions.month(name));
  }

  public void year(String name) {
    addPartitionColumn(name, "_year", Expressions.year(name));
  }

  public void truncate(String name, int width) {
    addPartitionColumn(name, "_trunc", Expressions.truncate(name, width));
  }

  private void addPartitionColumn(String name, String suffix, UnboundTerm term) {
    if (partitionColumns.containsKey(name)) {
      throw new IllegalStateException("Partition column with name " + name + " already exists");
    }
    partitionColumns.put(name, new PartitionColumn(name + suffix, term));
  }

  public Collection<PartitionColumn> getPartitionColumns() {
    return partitionColumns.values();
  }

  @Getter
  public static class PartitionColumn {
    private final String name;

    private final Term term;

    PartitionColumn(String name, Term term) {
      this.name = name;
      this.term = term;
    }
  }

  public static Stream<String> availablePartitionColumnSuffixes() {
    return Stream.of("", "_bucket", "_day", "_hour", "_month", "_year", "_trunc");
  }
}
