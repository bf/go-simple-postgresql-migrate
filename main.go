package main

import (
    "bufio"
    "context"
    "fmt"
    "io/ioutil"
    "os"
    "path"
    "regexp"
    "sort"
    "strings"
    "time"

    "github.com/jackc/pgx/v4"
)

const (
    DEFAULT_HOST     = "127.0.0.1"
    DEFAULT_PORT     = "5432"
    DEFAULT_USER     = "postgres"
    DEFAULT_PASSWORD = ""
    DEFAULT_DATABASE = "postgres"

    CONST_ENV_VAR_POSTGRESQL_USER = "POSTGRESQL_USER"
    CONST_ENV_VAR_POSTGRESQL_HOST = "POSTGRESQL_HOST"
    CONST_ENV_VAR_POSTGRESQL_PORT = "POSTGRESQL_PORT"
    CONST_ENV_VAR_POSTGRESQL_PASSWORD = "POSTGRESQL_PASSWORD"
    CONST_ENV_VAR_POSTGRESQL_PASSWORD_FILE = "POSTGRESQL_PASSWORD_FILE"
    CONST_ENV_VAR_POSTGRESQL_DATABASE = "POSTGRESQL_DATABASE"

    CONST_MIGRATIONS_FOLDER      = "postgresql-migrations"
    CONST_DATABASE_INFO_FILENAME = "postgresql-connection-string.txt"

    CONST_POSTGRESQL_TABLE_NAME   = "_go_simple_postgresql_migrate"
    CONST_POSTGRESQL_TABLE_SCHEMA = "CREATE TABLE IF NOT EXISTS %s (id serial, created_at timestamp with time zone DEFAULT NOW(), filename text, UNIQUE(filename))"

    CONST_TEMPLATE             = "--\n--   %s\n--\n-- created: %s\n--\n-- FORWARD (UP) migration is below this line:\n--\n\n\n%s\n\n"
    CONST_TEMPLATE_UNDO_MARKER = "\n--\n-- UNDO (DOWN) migration is below this line:\n-- (do not change this block!)\n--\n"
)

var postgreSQLConnection *pgx.Conn

// output help
func cmd_help() {
    fmt.Printf("%v {init|up|down|create name..|destroy}\n", os.Args[0])

    fmt.Println(`
    init        ask for database credentials and create migrations folder
    create      add a new migration file
    create-here add a new migration file in current folder (no checks)
    up          do forward migrations until database is up to date
    down        do exactly ONE backwards migration
    destroy     do all backwards migrations at once
    `)

    fmt.Printf(`
    Hint: Provide the PostgreSQL connection string via environment variables:
        %s (default: "%s")
        %s (default: "%s") or from file via %s
        %s (default: "%s")
        %s (default: "%s")
        %s (default: "%s")
    `, 
    CONST_ENV_VAR_POSTGRESQL_USER, DEFAULT_USER, 
    CONST_ENV_VAR_POSTGRESQL_PASSWORD, DEFAULT_PASSWORD, CONST_ENV_VAR_POSTGRESQL_PASSWORD_FILE,
    CONST_ENV_VAR_POSTGRESQL_DATABASE, DEFAULT_DATABASE,
    CONST_ENV_VAR_POSTGRESQL_HOST, DEFAULT_HOST, 
    CONST_ENV_VAR_POSTGRESQL_PORT, DEFAULT_PORT)

    os.Exit(0)
}

// log error messages
func logError(message string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, message+"\n", args...)
}

// read user input from STDIN (allows default value)
func readFromStdIn(what string, defaultValue string) string {
    reader := bufio.NewReader(os.Stdin)
    if len(defaultValue) > 0 {
        fmt.Printf("%s [%s]: ", what, defaultValue)
    } else {
        fmt.Printf("%s: ", what)
    }

    // read from STDIN
    userInput, _ := reader.ReadString('\n')
    userInput = strings.TrimSpace(userInput)

    // check if user typed something in
    if len(userInput) == 0 {
        if len(defaultValue) > 0 {
            return defaultValue
        } else {
            // need to try again
            return readFromStdIn(what, defaultValue)
        }
    }

    return userInput
}

// retrieve connection details from user
func getDatabaseConnectionStringFromUser() string {
    fmt.Println()
    fmt.Println("Please type in the PostgreSQL credentials you want to use:")

    // ask user to type in connection details
    host := readFromStdIn("host", DEFAULT_HOST)
    port := readFromStdIn("port", DEFAULT_PORT)
    user := readFromStdIn("user", DEFAULT_USER)
    password := readFromStdIn("password", DEFAULT_PASSWORD)
    database := readFromStdIn("database", DEFAULT_DATABASE)

    // convert into PostgreSQL connection string
    connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s",
        user, password, host, port, database)

    // if successful, return connection string
    return connectionString
}

// write string to file
func writeStringToFile(filePath string, strData string) {
    file, err := os.Create(filePath)
    if err != nil {
        logError("Error: unable to create file: %s", filePath)
        panic(err)
    }

    file.WriteString(strData)
    file.Close()
}

// get connection string from environment
func getDatabaseConnectionStringFromEnvironment() string {
    useConnectionStringFromEnvironment := false

    user := DEFAULT_USER
    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_USER)) > 0 {
        user = os.Getenv(CONST_ENV_VAR_POSTGRESQL_USER)
        useConnectionStringFromEnvironment = true
    }

    password := DEFAULT_PASSWORD
    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_PASSWORD)) > 0 {
        password = os.Getenv(CONST_ENV_VAR_POSTGRESQL_PASSWORD)
        useConnectionStringFromEnvironment = true
    }

    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_PASSWORD_FILE)) > 0 {
        filePath := os.Getenv(CONST_ENV_VAR_POSTGRESQL_PASSWORD_FILE)
        fileContent, err := ioutil.ReadFile(filePath)

        if err != nil {
            panic(err)
        }

        password = string(fileContent)
    }
    
    host := DEFAULT_HOST
    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_HOST)) > 0 {
        host = os.Getenv(CONST_ENV_VAR_POSTGRESQL_HOST)
        useConnectionStringFromEnvironment = true
    }

    port := DEFAULT_PORT
    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_PORT)) > 0 {
        port = os.Getenv(CONST_ENV_VAR_POSTGRESQL_PORT)
        useConnectionStringFromEnvironment = true
    }

    database := DEFAULT_DATABASE
    if len(os.Getenv(CONST_ENV_VAR_POSTGRESQL_DATABASE)) > 0 {
        database = os.Getenv(CONST_ENV_VAR_POSTGRESQL_DATABASE)
        useConnectionStringFromEnvironment = true
    }
    

    if !useConnectionStringFromEnvironment {
        return ""
    }

    return "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
}

// initiate the versioning
func cmd_init() {
    // check if migrations folder exists
    _, err := os.Stat(CONST_MIGRATIONS_FOLDER)

    // if not, then create it
    if os.IsNotExist(err) {
        os.Mkdir(CONST_MIGRATIONS_FOLDER, 0700)
        fmt.Println("created migrations folder", CONST_MIGRATIONS_FOLDER)
    }

    filePathDatabaseConnectionString := path.Join(CONST_MIGRATIONS_FOLDER, CONST_DATABASE_INFO_FILENAME)

    // check if database info has already been stored as file
    _, err = os.Stat(filePathDatabaseConnectionString)
    if !os.IsNotExist(err) {
        logError("Error: PostgreSQL connection information already stored in %s",
            filePathDatabaseConnectionString)
        logError("Hint: Remove the file if you want to continue")
        os.Exit(1)
    }

    // get connection info from environment variable
    connectionString := getDatabaseConnectionStringFromEnvironment()
    storeConnectionStringAsFile := false

    // ask user for connection info
    if len(connectionString) == 0 {
        connectionString = getDatabaseConnectionStringFromUser()
        storeConnectionStringAsFile = true
    }

    // attempt DB connection
    connectToPostgreSQL(connectionString)

    // store connection string in file
    if storeConnectionStringAsFile {
        writeStringToFile(filePathDatabaseConnectionString, connectionString)
    }

    // establish database connection
    connectToStoredDatabaseConnection()

    // create initial tables
    _, err = postgreSQLConnection.Exec(
        context.Background(),
        fmt.Sprintf(CONST_POSTGRESQL_TABLE_SCHEMA, CONST_POSTGRESQL_TABLE_NAME))
    if err != nil {
        logError("Error: Failed to create initial table")
        panic(err)
    }

    fmt.Println("Successfully set up migrations table at", CONST_POSTGRESQL_TABLE_NAME)

    os.Exit(0)
}

// get connection string from file
func getDatabaseConnectionStringFromFile() string {
    filePath := path.Join(CONST_MIGRATIONS_FOLDER, CONST_DATABASE_INFO_FILENAME)
    connectionString, err := ioutil.ReadFile(filePath)

    // file does not exist or cannot be read
    if err != nil {
        logError("Error: Could not read connection details from: %s", filePath)
        logError("Hint: Maybe you should run 'init' first?")
        panic(err)
    }

    return string(connectionString)
}

// attempt PostgreSQL connection and return db object
func connectToPostgreSQL(connectionString string) {
    var err error
    postgreSQLConnection, err = pgx.Connect(context.Background(), connectionString)
    if err != nil {
        logError("Error: Failed to create database connection with connection string %s", connectionString)
        panic(err)
    }
}

// retrieve database cursor
func connectToStoredDatabaseConnection() {
    // get connection info from environment variable
    connectionString := getDatabaseConnectionStringFromEnvironment()

    // fallback: attempt to read from file
    if len(connectionString) == 0 {
        connectionString = getDatabaseConnectionStringFromFile()
    }

    connectToPostgreSQL(connectionString)
}

// create new migration file
func cmd_create(fileName string) {
    // check if DB config file already exists
    filePath := path.Join(CONST_MIGRATIONS_FOLDER, CONST_DATABASE_INFO_FILENAME)
    _, err := os.Stat(filePath)
    if os.IsNotExist(err) {
        logError("Error: Database configuration file not found: %s", filePath)
        logError("Hint: Did you run the 'init' command? Are you in the wrong folder?")
        os.Exit(1)
    }

    // sanitize filename
    reFileName := regexp.MustCompile("[^a-zA-Z0-9-_]")
    sanitizedFileName := string(reFileName.ReplaceAll([]byte(strings.TrimSpace(fileName)), []byte("")))

    reTimestamp := regexp.MustCompile("[^0-9]")
    timestamp := time.Now().UTC()

    timestampForFileName := timestamp.Format(time.RFC3339)
    timestampForFileName = string(reTimestamp.ReplaceAll([]byte(timestampForFileName), []byte("")))

    migrationFileName := timestampForFileName + "-" + sanitizedFileName + ".sql"

    // check if file already exists
    filePath = path.Join(CONST_MIGRATIONS_FOLDER, migrationFileName)
    _, err = os.Stat(filePath)
    if !os.IsNotExist(err) {
        logError("Error: migration file does already exist: %s", filePath)
        os.Exit(1)
    }

    // write template to file
    writeStringToFile(filePath, fmt.Sprintf(CONST_TEMPLATE,
        sanitizedFileName,
        timestamp.Format(time.RFC850),
        CONST_TEMPLATE_UNDO_MARKER))

    fmt.Println("created", filePath)

    os.Exit(0)
}

// create new migration file right here in this folder
func cmd_create_here(fileName string) {
    // sanitize filename
    reFileName := regexp.MustCompile("[^a-zA-Z0-9-_]")
    sanitizedFileName := string(reFileName.ReplaceAll([]byte(strings.TrimSpace(fileName)), []byte("")))

    reTimestamp := regexp.MustCompile("[^0-9]")
    timestamp := time.Now().UTC()

    timestampForFileName := timestamp.Format(time.RFC3339)
    timestampForFileName = string(reTimestamp.ReplaceAll([]byte(timestampForFileName), []byte("")))

    migrationFileName := timestampForFileName + "-" + sanitizedFileName + ".sql"

    // check if file already exists
    workDir, _ := os.Getwd()
    filePath := path.Join(workDir, migrationFileName)
    _, err := os.Stat(filePath)
    if !os.IsNotExist(err) {
        logError("Error: migration file does already exist: %s", filePath)
        os.Exit(1)
    }

    // write template to file
    writeStringToFile(filePath, fmt.Sprintf(CONST_TEMPLATE,
        sanitizedFileName,
        timestamp.Format(time.RFC850),
        CONST_TEMPLATE_UNDO_MARKER))

    fmt.Println("created", filePath)

    os.Exit(0)
}


// fetch  migrations from database
func getMigrationsFromDatabase() []string {
    connectToStoredDatabaseConnection()

    rows, err := postgreSQLConnection.Query(context.Background(),
        fmt.Sprintf("SELECT filename FROM %s ORDER BY id ASC", CONST_POSTGRESQL_TABLE_NAME))
    if err != nil {
        logError("Error: could not read migrations from database table %s", CONST_POSTGRESQL_TABLE_NAME)
        panic(err)
    }

    var filename string
    var migrationsInDatabase []string
    for rows.Next() {
        err := rows.Scan(&filename)
        if err != nil {
            logError("Error: could not read migrations from database table %s: unable to scan row into filename", CONST_POSTGRESQL_TABLE_NAME)
            panic(err)
        }

        migrationsInDatabase = append(migrationsInDatabase, filename)
    }

    err = rows.Err()
    if err != nil {
        logError("Error: could not read migrations from database table %s: row error", CONST_POSTGRESQL_TABLE_NAME)
        panic(err)
    }

    return migrationsInDatabase
}

// fetch migrations from filesystem
func getMigrationsFromFileSystem() []string {
    files, err := ioutil.ReadDir(CONST_MIGRATIONS_FOLDER)
    if err != nil {
        panic(err)
    }

    reMigrationFile := regexp.MustCompile("^[0-9]{14}-[a-zA-Z0-9_-]+.sql$")

    var migrationsInFileSystem []string
    for _, file := range files {
        if reMigrationFile.MatchString(file.Name()) {
            migrationsInFileSystem = append(migrationsInFileSystem, file.Name())
        }
    }

    sort.Strings(migrationsInFileSystem)

    return migrationsInFileSystem
}

// read migration from file
func readMigrationFromFile(fileName string) (string, string) {
    filePath := path.Join(CONST_MIGRATIONS_FOLDER, fileName)
    fileContentBytes, err := ioutil.ReadFile(filePath)

    if err != nil {
        logError("Error: Could not read file %s", filePath)
        panic(err)
    }

    fileContent := string(fileContentBytes)

    // check if separator exists in in file
    if !strings.Contains(fileContent, CONST_TEMPLATE_UNDO_MARKER) {
        logError("Error: Could not find the separator in file %s", filePath)
        logError("Hint: Make sure this string splits up the up/down migration in the file:")
        logError(CONST_TEMPLATE_UNDO_MARKER)
        os.Exit(1)
    }

    // split file content into up/down migration
    arrParts := strings.Split(fileContent, CONST_TEMPLATE_UNDO_MARKER)

    // check if array has sane length
    if len(arrParts) != 2 {
        logError("Error: Found separator in file %s, but after splitting there is an array with %d elements instead of 2 as we expected.",
            filePath, len(arrParts))
        os.Exit(2)
    }

    sqlMigrationForward := cleanUpSQLString(arrParts[0])
    if len(sqlMigrationForward) == 0 {
        logError("Error: Forward (UP) migration is empty in file %s", filePath)
        os.Exit(3)
    }

    sqlMigrationBackward := cleanUpSQLString(arrParts[1])
    if len(sqlMigrationBackward) == 0 {
        logError("Error: Backward (DOWN) migration is empty in file %s", filePath)
        os.Exit(3)
    }

    return sqlMigrationForward, sqlMigrationBackward
}

// clean up SQL string read from migration file
func cleanUpSQLString(sqlString string) string {
    // remove SQL comments
    reSQLComments := regexp.MustCompile("(?m)^--[^\n]*$")
    sqlString = string(reSQLComments.ReplaceAll([]byte(sqlString), []byte("")))

    // remove whitespace
    sqlString = strings.TrimSpace(sqlString)

    return sqlString
}

// check consistency of migrations in database & local filesystem
func checkConsistencyOfDatabaseAndLocalFileSystem() ([]string, []string) {
    // read migrations files from local folder
    migrationsInFileSystem := getMigrationsFromFileSystem()

    // check if we have migrations at all
    if len(migrationsInFileSystem) == 0 {
        logError("Error: No migration files found in local folder %s", CONST_MIGRATIONS_FOLDER)
        logError("Hint: Maybe you need to run 'create' first?")
        os.Exit(1)
    }

    // check if local migration files are well-formed
    for _, fileNameFromFileSystem := range migrationsInFileSystem {
        _, _ = readMigrationFromFile(fileNameFromFileSystem)
    }

    // read migrations from database
    migrationsInDatabase := getMigrationsFromDatabase()

    // check if # of migrations makes sense
    if len(migrationsInDatabase) > len(migrationsInFileSystem) {
        logError("Error: Missing local migration files. There are more migrations stored in the database (%d) than in local folder %s (%d)",
            len(migrationsInDatabase), CONST_MIGRATIONS_FOLDER, len(migrationsInFileSystem))
        os.Exit(1)
    }

    // check if migrations listed in database also exist in file system
    for index, filenameFromDatabase := range migrationsInDatabase {
        if filenameFromDatabase != migrationsInFileSystem[index] {
            logError("Error: Migration stored in database at position #%d (%s) does not match local migration file %s",
                index, filenameFromDatabase, migrationsInFileSystem[index])
            os.Exit(2)
        }
    }

    return migrationsInFileSystem, migrationsInDatabase
}

// migrate towards latest version of db
func cmd_up() {
    // perform consistency checks
    migrationsInFileSystem, migrationsInDatabase := checkConsistencyOfDatabaseAndLocalFileSystem()

    // is there anything to do?
    if len(migrationsInDatabase) == len(migrationsInFileSystem) {
        fmt.Printf("Database already up to date, with %d migrations applied.\nMost recent migration is %s\n",
            len(migrationsInDatabase), migrationsInDatabase[len(migrationsInDatabase)-1])
        os.Exit(0)
    }

    // calculate delta
    delta := migrationsInFileSystem[len(migrationsInDatabase):]
    // fmt.Println("delta", delta)

    for _, fileName := range delta {
        // get sql for forward migration
        sqlMigrationForward, _ := readMigrationFromFile(fileName)

        // perform migration
        insertedId := migrateForward(fileName, sqlMigrationForward)

        fmt.Printf("forward migration: %s (database id: %d)\n", fileName, insertedId)
    }
}

// migrate forward
func migrateForward(fileName string, sqlMigrationForward string) int {
    tx, err := postgreSQLConnection.Begin(context.Background())
    if err != nil {
        logError("Error: Failed to start forward transaction")
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    defer tx.Rollback(context.Background())

    // execute sql code of migration
    _, err = tx.Exec(context.Background(), sqlMigrationForward)
    if err != nil {
        logError("Error: Forward transaction failed")
        logError("Error while processing file: %s", fileName)
        logError(sqlMigrationForward)
        panic(err)
    }

    // store migration in table
    var insertedId int
    err = tx.QueryRow(context.Background(),
        fmt.Sprintf("INSERT INTO %s (filename) VALUES ($1) RETURNING id", CONST_POSTGRESQL_TABLE_NAME),
        fileName).Scan(&insertedId)
    if err != nil {
        logError("Error: Failed to store forward migration info in %s", CONST_POSTGRESQL_TABLE_NAME)
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    err = tx.Commit(context.Background())
    if err != nil {
        logError("Error: Failed to commit forward transaction")
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    return insertedId
}

// migrate backwards
func migrateBackward(fileName string, sqlMigrationBackward string) {
    tx, err := postgreSQLConnection.Begin(context.Background())
    if err != nil {
        logError("Error: Failed to start backward transaction")
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    defer tx.Rollback(context.Background())

    // check that most recent transaction is the one we are trying to undo
    var mostRecentMigrationFileName string
    var mostRecentMigrationId int
    err = tx.QueryRow(context.Background(),
        fmt.Sprintf(
            "SELECT id, filename FROM %s ORDER BY created_at DESC LIMIT 1",
            CONST_POSTGRESQL_TABLE_NAME)).Scan(
        &mostRecentMigrationId, &mostRecentMigrationFileName)
    if err != nil {
        logError("Error: Cannot fetch most recent migration")
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    // execute sql code of migration
    _, err = tx.Exec(context.Background(), sqlMigrationBackward)
    if err != nil {
        logError("Error: background migration failed")
        logError("Error while processing file: %s", fileName)
        logError(sqlMigrationBackward)
        panic(err)
    }

    // store migration in table
    _, err = tx.Exec(context.Background(),
        fmt.Sprintf("DELETE FROM %s WHERE id = $1", CONST_POSTGRESQL_TABLE_NAME),
        mostRecentMigrationId)
    if err != nil {
        logError("Error: Failed to remove backward migration #%d from database table %s",
            mostRecentMigrationId, CONST_POSTGRESQL_TABLE_NAME)
        logError("Error while processing file: %s", fileName)
        panic(err)
    }

    err = tx.Commit(context.Background())
    if err != nil {
        logError("Error: Failed to commit backward transaction")
        logError("Error while processing file: %s", fileName)
        panic(err)
    }
}

// migrate one step backwards
func cmd_down() {
    // perform consistency checks
    _, migrationsInDatabase := checkConsistencyOfDatabaseAndLocalFileSystem()

    // is there anything to do?
    if len(migrationsInDatabase) == 0 {
        fmt.Println("There are no further migrations that can be reverted.")
        os.Exit(0)
    }

    // get filename of last migration from array
    mostRecentMigrationFileName := migrationsInDatabase[len(migrationsInDatabase)-1]

    // get the sql query
    _, sqlMigrationBackward := readMigrationFromFile(mostRecentMigrationFileName)

    // perform backwards migration with database transaction
    migrateBackward(mostRecentMigrationFileName, sqlMigrationBackward)

    fmt.Println("undo:", mostRecentMigrationFileName)
}

// migrate all steps backwards
func cmd_destroy() {
    for {
        cmd_down()
    }
}

func main() {
    if len(os.Args) < 2 {
        cmd_help()
    }

    switch os.Args[1] {
    case "init":
        if len(os.Args) == 2 {
            cmd_init()
        }

    case "create":
        cmd_create(strings.Join(os.Args[2:], "-"))

    case "create-here":
        cmd_create_here(strings.Join(os.Args[2:], "-"))

    case "up":
        if len(os.Args) == 2 {
            cmd_up()
        }

    case "down":
        if len(os.Args) == 2 {
            cmd_down()
        }

    case "destroy":
        if len(os.Args) == 2 {
            cmd_destroy()
        }

    default:
        cmd_help()
    }
}
