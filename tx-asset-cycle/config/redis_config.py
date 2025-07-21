import logging
import os

from utils.utilities import get_db_credentials_from_secrets_manager

logger = logging.getLogger(__name__)

SECRET_NAME = os.getenv("REDIS_CREDENTIALS_SECRET_NAME")
AWS_ACCESS_KEY_ID_ENV = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_ENV = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN_ENV = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION_NAME_ENV = os.getenv("AWS_REGION")

REDIS_DB_SETTINGS = get_db_credentials_from_secrets_manager(
    secret_name=SECRET_NAME,
    region_name=AWS_REGION_NAME_ENV,
    db_type='redis',
    aws_access_key_id=AWS_ACCESS_KEY_ID_ENV,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ENV,
    aws_session_token=AWS_SESSION_TOKEN_ENV
)
