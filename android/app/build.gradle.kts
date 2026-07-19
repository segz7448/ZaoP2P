plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.zao.p2p"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.zao.p2p"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "0.1.0-milestone1"

        ndk {
            // Match the targets built by cross/cargo-ndk in CI.
            abiFilters += listOf("arm64-v8a", "armeabi-v7a", "x86_64")
        }
    }

    // Release signing credentials come from environment variables, never
    // from anything committed to the repo -- CI injects them from GitHub
    // Secrets (see .github/workflows/android-build.yml), and locally
    // they'd come from your own shell env if you ever build a signed
    // release apk on your machine. If they're not set (e.g. a local
    // `assembleRelease` run without signing set up), the release build
    // type below falls back to no signing config at all -- same
    // unsigned-output behavior as before, just for local/dev use, not CI.
    val releaseKeystorePath = System.getenv("ZAO_RELEASE_KEYSTORE_PATH")
    val releaseKeystorePassword = System.getenv("ZAO_RELEASE_KEYSTORE_PASSWORD")
    val releaseKeyAlias = System.getenv("ZAO_RELEASE_KEY_ALIAS")
    val releaseKeyPassword = System.getenv("ZAO_RELEASE_KEY_PASSWORD")
    val hasReleaseSigningEnv = !releaseKeystorePath.isNullOrBlank() &&
        !releaseKeystorePassword.isNullOrBlank() &&
        !releaseKeyAlias.isNullOrBlank() &&
        !releaseKeyPassword.isNullOrBlank()

    if (hasReleaseSigningEnv) {
        signingConfigs {
            create("release") {
                storeFile = file(releaseKeystorePath!!)
                storePassword = releaseKeystorePassword
                keyAlias = releaseKeyAlias
                keyPassword = releaseKeyPassword
            }
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            if (hasReleaseSigningEnv) {
                signingConfig = signingConfigs.getByName("release")
            }
        }
    }

    sourceSets {
        getByName("main") {
            // Prebuilt .so files (produced by the Rust build step in CI)
            // land here before this Gradle build runs.
            jniLibs.srcDirs("src/main/jniLibs")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.appcompat:appcompat:1.7.0")
    implementation("com.google.android.material:material:1.12.0")
    implementation("androidx.recyclerview:recyclerview:1.3.2")
    implementation("androidx.activity:activity-ktx:1.9.0")
    implementation("androidx.documentfile:documentfile:1.0.1")
}
