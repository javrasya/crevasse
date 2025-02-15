package com.crevasse.plugin.helper;

import org.gradle.util.GradleVersion;

public enum GradleFeatures {
    projectIntoExtensionInjection() {
        public boolean isSupportedBy(GradleVersion version) {
            return version.compareTo(GradleVersions.v7_1) >= 0;
        }
    },
    objectFactoryFileCollection() {
        @Override
        public boolean isSupportedBy(GradleVersion version) {
            return version.compareTo(GradleVersions.v5_3) >= 0;
        }
    },
    configCache() {
        @Override
        public boolean isSupportedBy(GradleVersion version) {
            return version.compareTo(GradleVersions.v6_6) >= 0;
        }
    },
    getSourcesJarTaskName() {
        @Override
        public boolean isSupportedBy(GradleVersion version) {
            return version.compareTo(GradleVersions.v6_0) >= 0;
        }
    },
    ideaModuleTestSources() {
        @Override
        public boolean isSupportedBy(GradleVersion version) {
            return version.compareTo(GradleVersions.v7_4) >= 0;
        }
    };

    public abstract boolean isSupportedBy(GradleVersion version);

    boolean isSupported() {
        return isSupportedBy(GradleVersion.current());
    }
}