from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    db_port: int
    db_host: str

    elastic_port: int
    elastic_host: str

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )


settings = Settings()
