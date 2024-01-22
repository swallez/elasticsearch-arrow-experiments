plugins {
    id("java")
}

group = "co.elastic.clients"
version = "1.0-SNAPSHOT"

tasks.compileJava {
    options.release = 21
}

repositories {
    mavenCentral()
}

tasks.register<JavaExec>(name = "ingest-data") {
    group = "application"
    description = "Ingest some test data in Elasticsearch"
    classpath = java.sourceSets["main"].runtimeClasspath
    mainClass.set("co.elastic.es_flight.DataIngester")
}

tasks.register<JavaExec>(name = "flight-server") {
    group = "application"
    description = "Starts the Arrow Flight bridge"
    classpath = java.sourceSets["main"].runtimeClasspath
    jvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")
    mainClass.set("co.elastic.es_flight.server.ESFlightServer")
}

tasks.register<JavaExec>(name = "flight-client") {
    group = "application"
    description = "Runs the Arrow Flight test client"
    classpath = java.sourceSets["main"].runtimeClasspath
    jvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")
    mainClass.set("co.elastic.es_flight.client.ESFlightTestClient")
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    implementation("org.apache.arrow:arrow-vector:14.0.2")
    implementation("org.apache.arrow:arrow-memory-netty:14.0.2")
    implementation("org.apache.arrow:arrow-algorithm:14.0.2")
    implementation("org.apache.arrow:arrow-jdbc:14.0.2")
    implementation("org.apache.arrow:flight-core:14.0.2")
    implementation("org.apache.arrow:flight-grpc:14.0.2")
    implementation("org.apache.arrow:flight-sql:14.0.2")
    implementation("org.apache.arrow:arrow-compression:14.0.2")

    implementation("commons-cli:commons-cli:1.5.0")
    implementation("org.apache.commons:commons-csv:1.10.0")

    implementation("co.elastic.clients:elasticsearch-java:8.10.0")

    implementation("org.apache.commons:commons-configuration2:2.9.0")

    implementation("org.slf4j:slf4j-nop:2.0.9")

    val jacksonVersion = "2.13.3"

    implementation("com.fasterxml.jackson.dataformat", "jackson-dataformat-cbor", jacksonVersion)
}

tasks.test {
    useJUnitPlatform()
}
