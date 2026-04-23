import com.biron.gradleVersionCatalog.gitVersionProvider

plugins {
	alias(bironLibs.plugins.kotlin.jvm)
	alias(bironLibs.plugins.kotlin.plugin.spring)
	alias(bironLibs.plugins.spring.boot)
}

group = "com.biron"
version = gitVersionProvider().get()

repositories {
	mavenCentral()
	maven {
		name = "mavenBiron"
		url = uri(property("mavenBironUrl")!!)
		credentials(PasswordCredentials::class)
	}
}

apply(plugin = "io.spring.dependency-management")

dependencies {
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.data:spring-data-jdbc")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("ch.qos.logback:logback-classic")
	implementation(bironLibs.kotlin.logging)
	implementation(bironLibs.arrow.core)
	implementation(bironLibs.arrow.fx.coroutines)
	implementation(bironLibs.clickhouse.jdbc)
	implementation(bironLibs.clikt)
	implementation(libs.biron.singer.kotlin.core)
	implementation(libs.biron.singer.kotlin.models)

	testImplementation(bironLibs.bundles.kotest)
	testImplementation(bironLibs.kotest.extensions.spring)
	testImplementation(bironLibs.kotest.assertions.arrow)
	testImplementation(bironLibs.kotest.assertions.json)
	testImplementation(bironLibs.mockk)
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation(bironLibs.testcontainers.core)
	testImplementation(bironLibs.testcontainers.clickhouse)
	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
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
