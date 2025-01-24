import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldNotContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldInclude
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.containers.Network
import ru.yandex.clickhouse.ClickHouseDataSource
import java.io.File
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.util.logging.Logger
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

data class Config(
    val host: String,
    val username: String,
    val password: String,
    val port: Int,
    val database: String,
    val extra_active_tables: List<String> = emptyList(),
    val tablesToRecreate: List<String> = emptyList(),
    val batch_size: Int? = null,
    val insert_stream_timeout_sec: Int? = null,
    val translate_values : Boolean = false
)

class ProcessStreamTest : DescribeSpec({

    lateinit var container: ClickHouseContainer
    lateinit var jdbcTemplate: JdbcTemplate
    lateinit var network: Network
    val logger = Logger.getLogger(ProcessStreamTest::class.java.name)

    val initialConnInfo = Config(
        host = "clickhouse-server",
        port = 8123,
        database = "datayse",
        username = "user",
        password = "averysecurepassword"
    )
    beforeSpec {
        try {
            // Créer un réseau Docker partagé
            network = Network.newNetwork()

            // Lancer le conteneur ClickHouse
            container = ClickHouseContainer("clickhouse/clickhouse-server:24.1.5.6")
                .withNetwork(network)
                .withNetworkAliases("clickhouse-server")
                .apply { start() }

            val jdbcUrl = "jdbc:clickhouse://${container.host}:${container.getMappedPort(initialConnInfo.port)}"
            val dataSource = ClickHouseDataSource(jdbcUrl)
            jdbcTemplate = JdbcTemplate(dataSource)

            // Créer la base de données et l'utilisateur
            jdbcTemplate.execute("CREATE DATABASE IF NOT EXISTS ${initialConnInfo.database};")
            jdbcTemplate.execute("CREATE USER IF NOT EXISTS ${initialConnInfo.username} IDENTIFIED WITH plaintext_password BY '${initialConnInfo.password}';")
            jdbcTemplate.execute("GRANT ALL ON ${initialConnInfo.database}.* TO ${initialConnInfo.username};")
            jdbcTemplate.execute("GRANT ALTER, CREATE, DROP ON ${initialConnInfo.database}.* TO '${initialConnInfo.username}';")

        } catch (e: Exception) {
            logger.severe("Erreur lors du démarrage du conteneur ClickHouse : ${e.message}")
            throw e
        }
    }
    afterSpec {
        try {
            container.stop()
        } catch (e: Exception) {
            logger.severe("Erreur lors de l'arrêt du conteneur : ${e.message}")
            throw e
        }
    }
    beforeEach {
        jdbcTemplate.execute("DROP DATABASE IF EXISTS ${initialConnInfo.database};")
        jdbcTemplate.execute("CREATE DATABASE ${initialConnInfo.database};")
    }

    fun runDockerCommandWithPowershell(configFilePath: String, filePath: String, networkId: String) {
        try {
            val command = """
            powershell.exe -Command "Get-Content $filePath | docker run --rm -i --network $networkId -v $configFilePath:/config.json ghcr.io/biron-bi/target-clickhouse --config /config.json > state.jsonl"
        """.trimIndent()

            val process = ProcessBuilder("powershell.exe", "-Command", command)
                .redirectErrorStream(true)
                .start()

            process.inputStream.bufferedReader().use { reader ->
                println(reader.readText())
            }

            val exitCode = process.waitFor()
            if (exitCode != 0) {
                val errorMessage = process.inputStream.bufferedReader().use { it.readText() }
                throw Error("Docker command failed with exit code $exitCode: $errorMessage")
            }

        } catch (e: IOException) {
            println("Erreur lors de l'exécution de la commande Docker : ${e.message}")
            e.printStackTrace()
        }
    }

    fun configFile(initialConnInfo: Config): File {
        val config = File.createTempFile("test-config", ".json").apply {
            val baseConfig = """
        {
            "host": "${initialConnInfo.host}",
            "port": ${initialConnInfo.port},
            "database": "${initialConnInfo.database}",
            "username": "${initialConnInfo.username}",
            "password": "${initialConnInfo.password}",
            "extra_active_tables": ${initialConnInfo.extra_active_tables.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "tablesToRecreate": ${initialConnInfo.tablesToRecreate.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }},
            "batch_size": ${initialConnInfo.batch_size ?: "null"},
            "insert_stream_timeout_sec": ${initialConnInfo.insert_stream_timeout_sec ?: "null"}
            "translate_values": ${initialConnInfo.translate_values}
        }
        """.trimIndent()

            writeText(baseConfig)
        }
        return config
    }



    describe("outputStream") {
        it("should write state to passed outputStream") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_with_state.jsonl", network.id)

            // Vérification du fichier généré
            val stateFilePath = Paths.get("./state.jsonl")
            Files.exists(stateFilePath) shouldBe true

            val stateContent = Files.readAllLines(stateFilePath, Charset.forName("UTF-16"))
                .map { it.trimStart('\uFEFF') }

            stateContent.size shouldBe 2

            val expectedContent = listOf(
                """{"bookmarks":{"toto":"tata"},",currently_syncing":"tickets"}""",
                """{"bookmarks":{},"currently_syncing":null}"""
            )
            stateContent shouldBe expectedContent

        }
    }
    describe("Schemas") {
        it("should create schemas") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)

            val tables = jdbcTemplate.queryForList("SHOW TABLES FROM ${initialConnInfo.database}", String::class.java)
            println("Tables existantes dans la base de données '${initialConnInfo.database}' : $tables")

            tables.size shouldBe 21
            tables shouldContain "ticket_audits"
            tables shouldContain "ticket_audits__events__attachments"
            tables shouldContain "ticket_audits__metadata__notifications_suppressed_for"
            tables shouldContain "tickets"
            tables shouldContain "tickets__custom_fields"
        }

        it("should create schema with nullable scalar array") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_schema_array_nullable.jsonl", network.id)

            val query = """
                    SELECT name, type
                    FROM system.columns
                    WHERE table LIKE 'return_requests_%'
                      AND database = '${initialConnInfo.database}'
                      AND name = 'value'
                """.trimIndent()

            val result = jdbcTemplate.queryForList(query).map { row ->
                "${row["name"]}\t${row["type"]}"
            }.joinToString("\n")

            result shouldBe "value\tNullable(String)"
        }

        it("should create schema with nullable scalar array as ClickHouse array") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_schema_with_array.jsonl", network.id)
            //runDockerCommandWithPowershell(configFile.absolutePath, dataFilePath, network.id)

            val columnsQuery = """
                        SELECT name, type
                        FROM system.columns
                        WHERE database = '${initialConnInfo.database}'
                          AND table = 'query_log'
                    """.trimIndent()

            val columnsResult = jdbcTemplate.queryForList(columnsQuery).map { row ->
                "${row["name"]}\t${row["type"]}"
            }

            columnsResult[0] shouldBe "databases\tArray(String)"
            columnsResult[1] shouldBe "event_time\tDateTime"

            val dataQuery = """
                        SELECT databases
                        FROM ${initialConnInfo.database}.query_log
                    """.trimIndent()

            val dataResult = jdbcTemplate.queryForList(dataQuery).map { row ->
                val databases = row["databases"]
                when (databases) {
                    is ru.yandex.clickhouse.ClickHouseArray -> {
                        (databases.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
                    }

                    else -> databases.toString()
                }
            }
            dataResult[0] shouldBe "['kento', 'nanami']"
        }
        it("should create schemas which specifies cardinality") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_cardinality.jsonl", network.id)

            val query = """
                    show tables from ${initialConnInfo.database}
                """.trimIndent()

            val tables = jdbcTemplate.queryForList(query).map { row ->
                row.values.first().toString()
            }
            tables.size shouldBe 1
            tables shouldContain "users"

            val createTableQuery = """
                    SHOW CREATE TABLE ${initialConnInfo.database}.users
                """.trimIndent()

            val createTableOutput = jdbcTemplate.queryForList(createTableQuery).joinToString("\n") { row ->
                row.values.joinToString("\t")
            }

            println("Structure de la table 'users' :\n$createTableOutput")
            createTableOutput shouldContain "`name` LowCardinality(Nullable(String))"

        }
        it("should create schemas which specifiesPK") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_schema_with_all_pk.jsonl", network.id)

            val query = """
                    describe table ${initialConnInfo.database}.tickets__follower_ids
                """.trimIndent()
            val columns = jdbcTemplate.queryForList(query).map { row ->
                row.values.joinToString("\t")
            }

            columns[0] shouldInclude "_root_id"
            columns[1] shouldInclude "_parent_id"
            columns[2] shouldInclude "_level_0_index"
        }
        it("should do nothing if schemas already exists") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)

            val tablesAfter =
                jdbcTemplate.queryForList("SHOW TABLES FROM ${initialConnInfo.database}", String::class.java)
            println("Tables existantes dans la base de données '${initialConnInfo.database}' : $tablesAfter")

            tablesAfter.size shouldBe 21
        }
    }
    describe("columns update") {
        it("should create / update / delete columns if schema already exists and new has different columns") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_modified.jsonl", network.id)

            val columns = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")

            println("Columns : $columns")
            columns.size shouldBe 21

            val execResult = jdbcTemplate.queryForList(
                "select name, type\n" +
                        "from system.columns\n" +
                        "where table = 'tickets'\n" +
                        "and database = '${initialConnInfo.database}'\n" +
                        "order by name"
            ).map { row ->
                row.values.joinToString("\t")
            }.map { it.replace("\t", " ") }

            println("Columns : $execResult")

            execResult shouldContain "organization_id Nullable(String)"
            execResult shouldContain "new_requester_id Nullable(Int64)"
            execResult shouldNotContain "requester_id Nullable(Int64)"

        }
        it("should start by truncating before applying schema update") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_nullable.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_non_nullable.jsonl", network.id)

            val execResult = jdbcTemplate.queryForList(
                "select name, type\n" +
                        "from system.columns\n" +
                        "where table = 'users'\n" +
                        "and database = '${initialConnInfo.database}'\n" +
                        "order by name"
            ).map { row ->
                row.values.joinToString("\t")
            }.map { it.replace("\t", " ") }

            println("Columns : $execResult")
            execResult shouldContain "id Int64"
            execResult.size shouldBe 1
        }
        it("should handle state at the end of the stream + a closing state, launched several times") {
            val configFile = configFile(initialConnInfo)
            for (i in 0 until 10) {
                runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_with_state.jsonl", network.id)
                runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_tiny.jsonl", network.id)
            }

            val execResult = jdbcTemplate.queryForList("select * from ${initialConnInfo.database}.tickets").map { row ->
                row.values.joinToString("\t")
            }.map { it.replace("\t", ",") }

            println("Columns : $execResult")
            execResult.size shouldBe 3
            execResult[1] shouldBe "2,59"
        }
        it("should rename tables as dropped when they are no longer active, and exclude dropped and archived") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_inactive.jsonl", network.id)

            val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)
            println("Tables : $tables")
            tables.size shouldBe 21
            tables.forEach { table ->
                if (!table.contains("ticket_audits")) {
                    table.startsWith("_dropped_") shouldBe true
                    println("Table $table should start with '_dropped_'")
                } else {
                    table.startsWith("_dropped_") shouldBe false
                    println("Table $table should not start with '_dropped_'")
                }
            }
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_inactive.jsonl", network.id)
            val execResult =
                jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)
            execResult.size shouldBe 21
            execResult.forEach { table ->
                if (!table.contains("ticket_audits")) {
                    table.startsWith("_dropped_") shouldBe true
                    println("Table $table should start with '_dropped_'")
                } else {
                    table.startsWith("_dropped_") shouldBe false
                    println("Table $table should not start with '_dropped_'")
                }
                table.startsWith("_dropped__dropped_") shouldBe false
                println("table $table should not be renamed twice")
            }

            jdbcTemplate.execute("RENAME TABLE ${initialConnInfo.database}._dropped_ticket_metrics TO ${initialConnInfo.database}._archived_ticket_metrics")
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_inactive.jsonl", network.id)
            val showTables =
                jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)

            showTables.size shouldBe 21
            showTables.forEach { table ->
                if (!table.contains("ticket_audits")) {
                    if (table.contains("ticket_metrics")) {
                        table.startsWith("_archived_") shouldBe true
                        println("Table $table should start with '_archived_'")

                        table.contains("_dropped_") shouldBe false
                        println("Table $table should not include '_dropped_'")
                    } else {
                        table.startsWith("_dropped_") shouldBe true
                        println("Table $table should start with '_dropped_'")
                    }
                } else {
                    table.startsWith("_archived_") shouldBe false
                    println("Table $table should not start with '_archived_'")

                    table.startsWith("_dropped_") shouldBe false
                    println("Table $table should not start with '_dropped_'")
                }
                table.startsWith("_dropped__dropped_") shouldBe false
                println("Table $table should not be renamed twice")
            }
        }
        it("should not rename tables as dropped when they are no longer active if they are registered as extra_active") {
            val configFile = configFile(initialConnInfo.copy(extra_active_tables = listOf("tickets")))
            println(configFile.readText(Charsets.UTF_8))

            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_inactive.jsonl", network.id)
            val execResult =
                jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)

            execResult.size shouldBe 21
            println("Tables : $execResult")
            execResult.forEach { table ->
                if (!table.contains("ticket_audits") && !table.contains("tickets")) {
                    table.startsWith("_dropped_") shouldBe true
                    println("Table $table should start with '_dropped_'")
                } else {
                    table.startsWith("_dropped_") shouldBe false
                    println("Table $table should not start with '_dropped_'")
                }
            }
        }
        //ne fonctionne pas
        xit("should throw if schema already exists and new has different columns with incompatible type") {
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_vanilla.jsonl", network.id)

//            val error = shouldThrow<Error> {
//                runDockerCommandWithPowershell(
//                    configFile.absolutePath,
//                    "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_vanilla_with_incompatible_update.jsonl",
//                    network.id
//                )
//            }
//             println("error : $error")

            val exception = shouldThrowAny {
                runDockerCommandWithPowershell(
                    configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_vanilla_with_incompatible_update.jsonl", network.id)
            }

            println("Exception type: ${exception::class}")
            println("Exception message: ${exception.message}")
//            try{
//                runDockerCommandWithPowershell(
//                    configFile.absolutePath,
//                    "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_vanilla_with_incompatible_update.jsonl",
//                    network.id
//                )
//            }catch (e: Exception) {
//                println("Exception : $e")
//            }

        }
        xit("should throw if schema has no primary key but has array children"){
        }

        //a verifier
        it("should ignore second schema definition"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_multiple_schema.jsonl", network.id)
        }

        //possible de mieux verifier
        it("should recreate if schemas already exists, new is different but specified to be recreated"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)

            val newConfigFile = configFile(initialConnInfo.copy(tablesToRecreate = listOf("tickets")))
            println("newConfigFile : ${newConfigFile.readText(Charsets.UTF_8)}")
            runDockerCommandWithPowershell(newConfigFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1_modified.jsonl", network.id)

            val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")
            tables.size shouldBe 21
        }
        it("should handle additional nested array"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_nested_array.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_nested_array_additional.jsonl", network.id)
            val tables = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}")
            tables shouldContainExactly listOf(
                mapOf("name" to "users"),
                mapOf("name" to "users__roles"),
                mapOf("name" to "users__roles__scopes")
            )
        }
    }
    describe("Records") {
        it("should insert simple records"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_short.jsonl", network.id)

            val execResult = jdbcTemplate.queryForList("select brand_id from ${initialConnInfo.database}.tickets where assignee_id = 11")
            execResult shouldContainExactly listOf(mapOf("brand_id" to 22L))
        }
        // a finir la premiere requette recupere deja l'id
        xit("should insert record after some time even if stream isnt ended nor state message were received"){
            val schema = mapOf(
                "type" to "SCHEMA",
                "stream" to "tickets",
                "schema" to mapOf(
                    "properties" to mapOf(
                        "id" to mapOf("type" to listOf("integer"))
                    ), "type" to listOf("null", "object")
                ), "key_properties" to listOf("id")
            )
            val record = mapOf(
                "type" to "RECORD",
                "stream" to "tickets",
                "record" to mapOf("id" to 155)
            )

            val mapper = jacksonObjectMapper()
            val schemaJson = mapper.writeValueAsString(schema)
            val recordJson = mapper.writeValueAsString(record)
            val tempFile = File.createTempFile("schema", ".json").apply {
                writeText("$schemaJson\n$recordJson")
            }

            val config = configFile(initialConnInfo.copy(batch_size = 10, insert_stream_timeout_sec = 8))
            runDockerCommandWithPowershell(config.absolutePath, tempFile.absolutePath, network.id)
            Thread.sleep(1000)

            val execResult = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets")
            println ("execResult : $execResult")
            //execResult shouldContainExactly listOf(mapOf("id" to ""))
            Thread.sleep(4000)

            val execResults = jdbcTemplate.queryForList("select id from ${initialConnInfo.database}.tickets")
            println ("execResult : $execResults")
            execResults shouldContainExactly listOf(mapOf("id" to 155L))
        }
        it("should allow reordering of schema"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_short.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_short_reordered.jsonl", network.id)
            val execResult = jdbcTemplate.queryForList("select brand_id from ${initialConnInfo.database}.tickets where assignee_id = 11")
            execResult shouldContainExactly listOf(mapOf("brand_id" to 22L))
        }
        it("should flatten nested object"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_nested_object.jsonl", network.id)
            val execResult = jdbcTemplate.queryForList("select follower_ids__name from ${initialConnInfo.database}.tickets")
            execResult shouldContainExactly listOf(mapOf("follower_ids__name" to "jack"))
        }
        it("should ingest stream from real data: covidtracker"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/covidtracker.jsonl", network.id)
            val execResult = jdbcTemplate.queryForList("select sum(total_rows), sum(tables.total_bytes) from system.tables where database = '${initialConnInfo.database}'").map { row ->
                row.values.joinToString("\t")
            }
            execResult.get(0) shouldBe  "5789\t1334466"

            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/covidtracker.jsonl", network.id)
            val execResults = jdbcTemplate.queryForList("select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'").map { row ->
                row.values.joinToString("\t")
            }
            execResults.get(0) shouldBe  "5789"
        }
        it("should ingest stream from real data: clickhouse query log"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/clickhouse_query_log.jsonl", network.id)
            val execResult = jdbcTemplate.queryForList("select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'").map { row ->
                row.values.joinToString("\t")
            }
            execResult.get(0) shouldBe  "1"
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/clickhouse_query_log.jsonl", network.id)
            val execResult2 = jdbcTemplate.queryForList("select sum(total_rows) from system.tables where database = '${initialConnInfo.database}'").map { row ->
                row.values.joinToString("\t")
            }
            execResult2.get(0) shouldBe  "1"

            val execResult3 = jdbcTemplate.queryForList("select databases, `Settings.Names` from ${initialConnInfo.database}.query_log").map { row ->
                val databases = when (val db = row["databases"]) {
                    is ru.yandex.clickhouse.ClickHouseArray -> (db.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
                    else -> db.toString()
                }
                val settings = when (val st = row["Settings.Names"]) {
                    is ru.yandex.clickhouse.ClickHouseArray -> (st.array as Array<*>).joinToString(prefix = "[", postfix = "]") { "'$it'" }
                    else -> st.toString()
                }
                "$databases\t$settings"
            }
            execResult3.first() shouldBe "['system']\t['max_block_size', 'max_query_size', 'join_use_nulls', 'http_receive_timeout', 'max_expanded_ast_elements', 'max_memory_usage', 'max_parser_depth', 'lock_acquire_timeout']"
        }


    }
})