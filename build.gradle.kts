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
	implementation(bironLibs.kotlin.logging)
	implementation(bironLibs.arrow.core)
	implementation(bironLibs.clickhouse.jdbc)

	testImplementation(bironLibs.bundles.kotest)
	testImplementation(bironLibs.kotest.extensions.spring)
	testImplementation(bironLibs.kotest.assertions.arrow)
	testImplementation(bironLibs.mockk)
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation(bironLibs.testcontainers.core)
	testImplementation(bironLibs.testcontainers.clickhouse)
	testImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.5.1")
	testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.0")
}

tasks.test {
	useJUnitPlatform()
}
kotlin {
	jvmToolchain(21)
}
