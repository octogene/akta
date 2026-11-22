// The modules' own builds are in akta-core/ and akta-exposed/.
plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
}

allprojects {
    group = "dev.octogene"
    version = "0.1.0"
}
