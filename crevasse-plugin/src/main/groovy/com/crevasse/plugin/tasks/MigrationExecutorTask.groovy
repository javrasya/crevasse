package com.crevasse.plugin.tasks

import org.gradle.api.Action
import org.gradle.api.Task
import org.gradle.api.tasks.JavaExec

class MigrationExecutorTask extends JavaExec {


    @Override
    Task doLast(Action<? super Task> action) {
        mainClass.set("com.crevasse.plugin.tasks.MigrationExecutorTask")
        args("arg1", "arg2")
        return super.doLast(action)
    }
}
