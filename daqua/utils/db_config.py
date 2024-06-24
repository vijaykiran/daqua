class DBConfig:
    def __init__(self, host, port, database, username, password):
        self.connection_properties = {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver",
        }
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
