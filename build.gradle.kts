plugins {
    kotlin("jvm") version "2.0.10"
    id("org.jetbrains.kotlinx.atomicfu") version "0.25.0"
}

group = "com.esh.jbr-test"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:lincheck:2.34")
}

tasks.test {
    useJUnitPlatform()
}
