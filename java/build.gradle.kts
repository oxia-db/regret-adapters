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
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/regret-io/regret")
        credentials {
            username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String? ?: ""
            password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key") as String? ?: ""
        }
    }
}

dependencies {
    implementation(project(":java-sdk"))
    implementation("io.github.oxia-db:oxia-client:0.7.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")
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
