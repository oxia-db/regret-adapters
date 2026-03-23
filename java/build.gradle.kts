plugins {
    java
    application
    id("com.gradleup.shadow") version "9.0.0-beta12"
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}

repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.regret-io.regret:sdk-java:main-SNAPSHOT")
    implementation("io.streamnative.oxia:oxia-client:0.4.11")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("io.regret.adapter.oxia.OxiaKVAdapter")
}

tasks.shadowJar {
    archiveBaseName.set("regret-adapter-oxia")
    archiveClassifier.set("all")
    mergeServiceFiles()
}
