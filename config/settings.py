from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    BOT_TOKEN: str
    BOT_WEBHOOK_URL: str

    RABBIT_HOST: str
    RABBIT_PORT: int
    RABBIT_USER: str
    RABBIT_PASSWORD: str

    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    REDIS_HOST: str
    REDIS_PORT: int

    # MINIO_ENDPOINT: str
    # MINIO_ACCESS_KEY: str
    # MINIO_SECRET_KEY: str
    # MINIO_BUCKET: str = "photos-{user_id}"
    CELERY_BROKER_URL: str = None
    CELERY_RESULT_BACKEND: str = None

    USER_QUEUE: str = "user.response.{user_id}"

    @property
    def db_url(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def rabbit_url(self) -> str:
        return f"amqp://{self.RABBIT_USER}:{self.RABBIT_PASSWORD}@{self.RABBIT_HOST}:{self.RABBIT_PORT}/"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    # @property
    # def minio_url(self) -> str:
    #     return f"http://{self.MINIO_ENDPOINT}/{self.MINIO_BUCKET}"

    # @property
    # def celery_broker_url(self) -> str:
    #     return self.CELERY_BROKER_URL or self.rabbit_url

    # @property
    # def celery_result_backend(self) -> str:
    #     return self.CELERY_RESULT_BACKEND or self.redis_url

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
