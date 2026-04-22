pluginManagement {
	repositories {
		maven {
			name = "mavenBiron"
			url = uri(providers.gradleProperty("mavenBironUrl"))
			credentials(PasswordCredentials::class)
		}
		gradlePluginPortal()
	}
}
plugins {
	id("com.biron.biron-gradle-version-catalog") version "2.13.0"
}
rootProject.name = "singer-target-clickhouse"

