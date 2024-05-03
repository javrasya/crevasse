package com.crevasse.iceberg;

import com.crevasse.relocated.com.google.common.base.Preconditions;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import groovy.lang.Script;
import java.io.Serializable;

public class MigrationBaseScript extends Script implements Serializable {

  private Closure closure;

  public void migrate(@DelegatesTo(MigrationStep.class) Closure closure) {
    this.closure = closure;
  }

  void execute() {
    MigrationStep step = (MigrationStep) getBinding().getVariable("step");
    Preconditions.checkNotNull(this.closure, "No migration is defined");
    this.closure.setDelegate(step);
    this.closure.call();
  }

  @Override
  public Object run() {
    return null;
  }
}
