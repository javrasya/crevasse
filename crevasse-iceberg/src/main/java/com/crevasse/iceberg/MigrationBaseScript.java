package com.crevasse.iceberg;

import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import groovy.lang.Script;
import java.io.Serializable;

public class MigrationBaseScript extends Script implements Serializable {

  public void migrate(@DelegatesTo(MigrationStep.class) Closure closure) {
    MigrationStep step = (MigrationStep) getBinding().getVariable("step");
    closure.setDelegate(step);
    closure.call();
  }

  @Override
  public Object run() {
    return null;
  }
}
