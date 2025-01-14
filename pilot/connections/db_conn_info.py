from pydantic import BaseModel, Field


class DBConfig(BaseModel):
    db_type: str
    db_name: str
    file_path: str = ""
    db_host: str = ""
    db_port: int = 0
    db_user: str = ""
    db_pwd: str = ""
    comment: str = ""
