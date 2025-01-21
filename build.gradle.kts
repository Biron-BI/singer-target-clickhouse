plugins {
	alias(libs.plugins.kotlin.jvm)
	alias(libs.plugins.kotlin.plugin.spring)
	alias(libs.plugins.spring.boot)
	alias(libs.plugins.gitVersion)
}

group = "com.biron"
val gitVersion: groovy.lang.Closure<String> by extra // from https://github.com/palantir/gradle-git-version
version = gitVersion().removePrefix("v")

repositories {
	mavenCentral()
}

apply(plugin = "io.spring.dependency-management")

dependencies {
	implementation(libs.arrow)
	implementation(libs.kotlin.logging)
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.data:spring-data-jdbc")
	implementation(libs.clickhouse.jdbc)
	implementation(libs.httpclient5)
	implementation(libs.lz4.java)

	testImplementation(libs.bundles.kotest)
	testImplementation(libs.kotest.arrow)
	testImplementation(libs.kotest.spring)
	testImplementation(libs.mockk)
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation(libs.testcontainers)
	testImplementation(libs.testcontainers.clickhouse)

	testImplementation("ru.yandex.clickhouse:clickhouse-jdbc:0.3.1")

}

tasks.test {
	useJUnitPlatform()
}
kotlin {
	jvmToolchain(17)
}
