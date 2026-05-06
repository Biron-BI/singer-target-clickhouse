plugins {
	alias(libs.plugins.kotlin.jvm)
	alias(libs.plugins.kotlin.plugin.spring)
	alias(libs.plugins.spring.boot)
	alias(libs.plugins.kover)
	alias(libs.plugins.palantir.git.version)
}

val gitVersion: groovy.lang.Closure<String> by extra

group = "com.biron"
version = gitVersion().removePrefix("v")

repositories {
	mavenCentral()
}

apply(plugin = "io.spring.dependency-management")

dependencies {
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.data:spring-data-jdbc")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("ch.qos.logback:logback-classic")
	implementation(libs.kotlin.logging)
	implementation(libs.arrow.core)
	implementation(libs.arrow.fx.coroutines)
	implementation(libs.clickhouse.jdbc)
	implementation(libs.clikt)

	testImplementation(libs.kotest.runner.junit5)
	testImplementation(libs.kotest.assertions.core)
	testImplementation(libs.kotest.property)
	testImplementation(libs.kotest.framework.datatest)
	testImplementation(libs.kotest.extensions.spring)
	testImplementation(libs.kotest.assertions.arrow)
	testImplementation(libs.kotest.assertions.json)
	testImplementation(libs.mockk)
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation(libs.testcontainers.core)
	testImplementation(libs.testcontainers.clickhouse)
	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
	testImplementation(libs.jimfs)
}

tasks.test {
	useJUnitPlatform()
}
kotlin {
	jvmToolchain(21)
}

tasks.getByName<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
	archiveFileName.set("${archiveBaseName.get()}.${archiveExtension.get()}")
	manifest.attributes("Implementation-Version" to project.version)
}
