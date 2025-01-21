import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.collections.shouldContain
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


data class Config(
    val host: String,
    val username: String,
    val password: String,
    val port: Int,
    val database: String
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
            if (exitCode == 0) {
                println("Commande exécutée avec succès")
            } else {
                println("La commande a échoué avec le code de sortie : $exitCode")
            }

        } catch (e: IOException) {
            println("Erreur lors de l'exécution de la commande Docker : ${e.message}")
            e.printStackTrace()
        }
    }

    fun configFile(initialConnInfo: Config): File {
        val config = File.createTempFile("test-config", ".json").apply {
            writeText(
                """
            {
                "host": "${initialConnInfo.host}",
                "port": "${initialConnInfo.port}",
                "database": "${initialConnInfo.database}",
                "username": "${initialConnInfo.username}",
                "password": "${initialConnInfo.password}"
            }
            """.trimIndent()
            )
        }
        println("Fichier de configuration temporaire créé : ${config.absolutePath}")
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

            val columnsResult = jdbcTemplate.queryForList(columnsQuery).map{ row ->
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
        it("should create schemas which specifies cardinality"){
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
            createTableOutput shouldContain  "`name` LowCardinality(Nullable(String))"

        }
        it("should create schemas which specifiesPK"){
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
        it("should do nothing if schemas already exists"){
            val configFile = configFile(initialConnInfo)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)
            runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_1.jsonl", network.id)

            val tablesAfter = jdbcTemplate.queryForList("SHOW TABLES FROM ${initialConnInfo.database}", String::class.java)
            println("Tables existantes dans la base de données '${initialConnInfo.database}' : $tablesAfter")

            tablesAfter.size shouldBe 21
        }
    }
    describe("columns update"){
        it("should create / update / delete columns if schema already exists and new has different columns"){
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
                    "order by name").map { row ->
                        row.values.joinToString("\t")
                    }.map { it.replace("\t", " ") }

            println ("Columns : $execResult")

            execResult shouldContain "organization_id Nullable(String)"
            execResult shouldContain "new_requester_id Nullable(Int64)"
            execResult shouldNotContain "requester_id Nullable(Int64)"

        }
        it("should start by truncating before applying schema update"){
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

            println ("Columns : $execResult")
            execResult shouldContain "id Int64"
            execResult.size shouldBe  1
        }
        it("should handle state at the end of the stream + a closing state, launched several times"){
            val configFile = configFile(initialConnInfo)
            for (i in 0 until 10) {
                runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_with_state.jsonl", network.id)
                runDockerCommandWithPowershell(configFile.absolutePath, "./src/test/kotlin/com/biron/singerTargetClickHouse/utilsTest/kotest/data/stream_tiny.jsonl", network.id)
            }

            val execResult = jdbcTemplate.queryForList("select * from ${initialConnInfo.database}.tickets").map { row ->
                row.values.joinToString("\t")
            }.map { it.replace("\t", ",") }

            println ("Columns : $execResult")
            execResult.size shouldBe 3
            execResult[1] shouldBe "2,59"
        }
        it("should rename tables as dropped when they are no longer active, and exclude dropped and archived"){
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
            val execResult = jdbcTemplate.queryForList("show tables from ${initialConnInfo.database}", String::class.java)
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
            // a finir
        }
    }
})
