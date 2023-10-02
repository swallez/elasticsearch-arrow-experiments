plugins {
    id("java")
}

group = "co.elastic.clients"
version = "1.0-SNAPSHOT"

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

    implementation("org.apache.arrow:arrow-vector:13.0.0")
    implementation("org.apache.arrow:arrow-memory-netty:13.0.0")
    implementation("org.apache.arrow:arrow-algorithm:13.0.0")
    implementation("org.apache.arrow:arrow-jdbc:13.0.0")
    implementation("org.apache.arrow:flight-core:13.0.0")
    implementation("org.apache.arrow:flight-grpc:13.0.0")
    implementation("org.apache.arrow:flight-sql:13.0.0")

    implementation("commons-cli:commons-cli:1.5.0")
    implementation("org.apache.commons:commons-csv:1.10.0")

    implementation("co.elastic.clients:elasticsearch-java:8.10.0")

    implementation("org.apache.commons:commons-configuration2:2.9.0")
}

tasks.test {
    useJUnitPlatform()
}
