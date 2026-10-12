SELECT
    @@SERVERNAME AS server_name,
    DB_NAME() AS current_database,
    @@VERSION AS sql_server_version;