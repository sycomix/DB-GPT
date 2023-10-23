import os
import duckdb
from typing import List

default_db_path = os.path.join(os.getcwd(), "message")
duckdb_path = os.getenv(
    "DB_DUCKDB_PATH", f"{default_db_path}/connect_config.db"
)
table_name = "connect_config"


class DuckdbConnectConfig:
    def __init__(self):
        os.makedirs(default_db_path, exist_ok=True)
        self.connect = duckdb.connect(duckdb_path)
        self.__init_config_tables()

    def __init_config_tables(self):
        # check config table
        result = self.connect.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table_name]
        ).fetchall()

        if not result:
            # create config table
            self.connect.execute(
                "CREATE TABLE connect_config (id integer primary key, db_name VARCHAR(100) UNIQUE, db_type VARCHAR(50),  db_path VARCHAR(255) NULL, db_host VARCHAR(255) NULL,  db_port INTEGER NULL,  db_user VARCHAR(255) NULL,  db_pwd VARCHAR(255) NULL,  comment TEXT NULL)"
            )
            self.connect.execute("CREATE SEQUENCE seq_id START 1;")

    def add_url_db(
        self,
        db_name,
        db_type,
        db_host: str,
        db_port: int,
        db_user: str,
        db_pwd: str,
        comment: str = "",
    ):
        try:
            cursor = self.connect.cursor()
            cursor.execute(
                "INSERT INTO connect_config(id, db_name, db_type, db_path, db_host, db_port, db_user, db_pwd, comment)VALUES(nextval('seq_id'),?,?,?,?,?,?,?,?)",
                [db_name, db_type, "", db_host, db_port, db_user, db_pwd, comment],
            )
            cursor.commit()
            self.connect.commit()
        except Exception as e:
            print(f"add db connect info error1！{str(e)}")

    def get_file_db_name(self, path):
        try:
            conn = duckdb.connect(path)
            return conn.execute("SELECT current_database()").fetchone()[0]
        except Exception as e:
            raise f"Unusable duckdb database path:{path}"

    def add_file_db(self, db_name, db_type, db_path: str, comment: str = ""):
        try:
            cursor = self.connect.cursor()
            cursor.execute(
                "INSERT INTO connect_config(id, db_name, db_type, db_path, db_host, db_port, db_user, db_pwd, comment)VALUES(nextval('seq_id'),?,?,?,?,?,?,?,?)",
                [db_name, db_type, db_path, "", "", "", "", comment],
            )
            cursor.commit()
            self.connect.commit()
        except Exception as e:
            print(f"add db connect info error2！{str(e)}")

    def delete_db(self, db_name):
        cursor = self.connect.cursor()
        cursor.execute("DELETE FROM connect_config where db_name=?", [db_name])
        cursor.commit()
        return True

    def get_db_config(self, db_name):
        if not os.path.isfile(duckdb_path):
            return {}
        cursor = duckdb.connect(duckdb_path).cursor()
        if db_name:
            cursor.execute(
                "SELECT * FROM connect_config where db_name=? ", [db_name]
            )
        else:
            raise ValueError(f"Cannot get database by name{db_name}")

        fields = [field[0] for field in cursor.description]
        row_1 = list(cursor.fetchall()[0])
        return {field: row_1[i] for i, field in enumerate(fields)}

    def get_db_list(self):
        if not os.path.isfile(duckdb_path):
            return []
        cursor = duckdb.connect(duckdb_path).cursor()
        cursor.execute("SELECT db_name, db_type, comment FROM connect_config ")

        fields = [field[0] for field in cursor.description]
        data = []
        for row in cursor.fetchall():
            row_dict = {field: row[i] for i, field in enumerate(fields)}
            data.append(row_dict)
        return data

    def get_db_names(self):
        if os.path.isfile(duckdb_path):
            cursor = duckdb.connect(duckdb_path).cursor()
            cursor.execute("SELECT db_name FROM connect_config ")
            return [row[0] for row in cursor.fetchall()]
        return []
