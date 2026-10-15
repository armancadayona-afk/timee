plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}
val configuredServer = providers.gradleProperty("serverBaseUrl")
    .orElse(providers.environmentVariable("SERVER_BASE_URL"))
    .orElse("https://CHANGE-ME.invalid")
    .get()
android {
    namespace = "com.kittyworktimer"
    compileSdk = 35
    defaultConfig {
        applicationId = "com.kittyworktimer"
        minSdk = 26
        targetSdk = 35
        versionCode = 1
        versionName = "1.0.0"
        buildConfigField("String", "SERVER_BASE_URL", "\"$configuredServer\"")
    }
    flavorDimensions += "role"
    productFlavors {
        create("admin") {
            dimension = "role"
            applicationId = "com.kittyworktimer.admin"
            resValue("string", "app_name", "Kitty Timer Admin")
            buildConfigField("String", "DEFAULT_USERNAME", "\"admin\"")
        }
        create("worker1") {
            dimension = "role"
            applicationId = "com.kittyworktimer.worker1"
            resValue("string", "app_name", "Kitty Worker 1")
            buildConfigField("String", "DEFAULT_USERNAME", "\"worker1\"")
        }
        create("worker2") {
            dimension = "role"
            applicationId = "com.kittyworktimer.worker2"
            resValue("string", "app_name", "Kitty Worker 2")
            buildConfigField("String", "DEFAULT_USERNAME", "\"worker2\"")
        }
        create("worker3") {
            dimension = "role"
            applicationId = "com.kittyworktimer.worker3"
            resValue("string", "app_name", "Kitty Worker 3")
            buildConfigField("String", "DEFAULT_USERNAME", "\"worker3\"")
        }
    }
    buildFeatures { buildConfig = true }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}
