from ydb import issues
import atexit
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import aioboto3
import grpc
import jwt
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException, UploadFile, Form
from pydantic import BaseModel
from yandex.cloud.iam.v1.iam_token_service_pb2 import CreateIamTokenRequest
from yandex.cloud.iam.v1.iam_token_service_pb2_grpc import IamTokenServiceStub
from ydb import Driver, DriverConfig, SessionPool, SchemeError, credentials
from typing import Optional, Dict, Any
import requests 
from dotenv import load_dotenv
from config import Config
from fastapi import FastAPI, UploadFile, File, Form, HTTPException


load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()


class UserCreate(BaseModel):
    username: str


def normalize_username(value: str) -> str:
    return value.strip()


def serialize_last_seen(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)

def ensure_local_storage() -> None:
    base_dir = Path(Config.LOCAL_STORAGE_DIR)
    for subdir in ("photos", "csv", "videos", "trajectories"):
        (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    db_path = Path(Config.LOCAL_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

def get_sqlite_conn():
    return sqlite3.connect(Config.LOCAL_DB_PATH)

def init_sqlite():
    with get_sqlite_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS client_data (
                id TEXT NOT NULL,
                username TEXT NOT NULL,
                photo_path TEXT,
                csv_path TEXT,
                video_path TEXT,
                trajectory_path TEXT,
                created_at TEXT,
                PRIMARY KEY (id, username)
            )
        """)

def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)

class YandexCloudIAM:
    def __init__(
        self,
        oauth_token: Optional[str] = None,
        service_account_key: Optional[Dict[str, Any]] = None,
        service_account_key_file: Optional[str] = None,
        iam_token: Optional[str] = None,
        expires_at: Optional[datetime] = None
    ):
        """
        Инициализация менеджера IAM токенов Yandex Cloud.
        
        Поддерживает три способа аутентификации:
        1. OAuth токен пользователя
        2. Ключ сервисного аккаунта (переданный напрямую)
        3. Файл с ключом сервисного аккаунта
        """
        self.oauth_token = oauth_token or os.getenv("YC_OAUTH_TOKEN")
        self.service_account_key = service_account_key
        self.service_account_key_file = service_account_key_file or os.getenv("YC_SA_KEY_FILE")
        self.iam_token = iam_token
        self.expires_at = expires_at
        
        if self.service_account_key_file and not self.service_account_key:
            self._load_service_account_key()

    def _load_service_account_key(self) -> None:
        """Загружает ключ сервисного аккаунта из файла"""
        try:
            with open(f"/etc/secrets/{self.service_account_key_file}", 'r') as f:
                self.service_account_key = json.load(f)
            logger.info("Service account key loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load service account key file: {str(e)}")
            raise ValueError(f"Failed to load service account key file: {str(e)}")

    def _refresh_token(self) -> None:
        """Обновляет IAM токен (для использования в планировщике)"""
        try:
            if self.oauth_token:
                self._get_iam_token_via_oauth()
            elif self.service_account_key:
                self._get_iam_token_via_sa_key()
            logger.info("IAM token refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh IAM token: {str(e)}")
            raise

    def get_iam_token(self) -> str:
        """
        Возвращает действительный IAM токен.
        Автоматически обновляет токен при необходимости.
        """
        if self.iam_token and self.expires_at and self.expires_at > datetime.utcnow():
            return self.iam_token
            
        if self.oauth_token:
            return self._get_iam_token_via_oauth()
        elif self.service_account_key:
            return self._get_iam_token_via_sa_key()
        else:
            raise ValueError(
                "No authentication method provided. "
                "Please provide either OAuth token or service account key."
            )

    def _get_iam_token_via_oauth(self) -> str:
        """Получает IAM токен с помощью OAuth токена пользователя"""
        url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
        headers = {"Content-Type": "application/json"}
        payload = {"yandexPassportOauthToken": self.oauth_token}
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            self.iam_token = data["iamToken"]
            self.expires_at = datetime.utcnow() + timedelta(hours=1)
            logger.info("IAM token obtained via OAuth")
            return self.iam_token
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Invalid OAuth token. Please check your YC_OAUTH_TOKEN")
                raise PermissionError("Invalid OAuth token. Please check your YC_OAUTH_TOKEN")
            logger.error(f"IAM token request failed with status {e.response.status_code}")
            raise Exception(f"IAM token request failed with status {e.response.status_code}")
        except Exception as e:
            logger.error(f"Failed to get IAM token via OAuth: {str(e)}")
            raise Exception(f"Failed to get IAM token via OAuth: {str(e)}")

    def _get_iam_token_via_sa_key(self) -> str:
        """Получает IAM токен с помощью ключа сервисного аккаунта"""
        try:
            channel = grpc.secure_channel(
                'iam.api.cloud.yandex.net:443',
                grpc.ssl_channel_credentials()
            )
            stub = IamTokenServiceStub(channel)
            
            request = CreateIamTokenRequest(
                jwt=self._generate_jwt(self.service_account_key)
            )
            
            response = stub.Create(request)
            self.iam_token = response.iam_token
            self.expires_at = datetime.utcnow() + timedelta(seconds=response.expires_at.seconds)
            logger.info("IAM token obtained via service account")
            return self.iam_token
            
        except Exception as e:
            logger.error(f"Failed to get IAM token via service account: {str(e)}")
            raise Exception(f"Failed to get IAM token via service account: {str(e)}")

    @staticmethod
    def _generate_jwt(sa_key: Dict[str, Any]) -> str:
        """
        Генерирует JWT для аутентификации сервисного аккаунта.
        
        :param sa_key: Словарь с данными ключа сервисного аккаунта
        :return: Сгенерированный JWT токен
        """
        now = int(time.time())
        payload = {
            "aud": "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            "iss": sa_key["service_account_id"],
            "iat": now,
            "exp": now + 3600
        }
        
        return jwt.encode(
            payload,
            sa_key["private_key"],
            algorithm="PS256",
            headers={"kid": sa_key["id"]}
        )

    def is_token_valid(self) -> bool:
        """Проверяет, действителен ли текущий IAM токен"""
        return bool(self.iam_token) and self.expires_at and self.expires_at > datetime.utcnow()


class YDBConnection:
    def __init__(self, token_manager):
        self.token_manager = token_manager
        self.driver = None
        self.pool = None
    
    def connect(self):
        try:
            driver_config = DriverConfig(
                endpoint=Config.YDB_ENDPOINT,
                database=Config.YDB_DATABASE,
                credentials=credentials.AuthTokenCredentials(self.token_manager.get_iam_token())
            )
            self.driver = Driver(driver_config)
            self.driver.wait(timeout=5)
            self.pool = SessionPool(self.driver)
            logger.info("YDB connection established")
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}", exc_info=True)
            raise


def setup_scheduler(token_manager: YandexCloudIAM) -> BackgroundScheduler:
    """Настраивает планировщик для автоматического обновления токена"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        token_manager._refresh_token,
        'interval',
        hours=1,
        next_run_time=datetime.now()
    )
    atexit.register(scheduler.shutdown)
    return scheduler


token_manager = None
scheduler = None
ydb = None

if Config.LOCAL_DEBUG:
    logger.info("LOCAL_DEBUG enabled: using local storage and SQLite")
else:
    try:
        Config.validate()
        token_manager = YandexCloudIAM(service_account_key_file="authorized_key-5.json")
        scheduler = setup_scheduler(token_manager)
        ydb = YDBConnection(token_manager)
    except Exception as e:
        logger.critical(f"Initialization failed: {str(e)}")
        raise


@app.on_event("startup")
async def startup_event():
    try:
        if Config.LOCAL_DEBUG:
            ensure_local_storage()
            init_sqlite()
            logger.info("Local storage and SQLite initialized")
        else:
            scheduler.start()
            ydb.connect()
            
            def init_table(session):
                try:
                    session.execute_scheme('''
                        CREATE TABLE IF NOT EXISTS client_data (
                            id Text NOT NULL,
                            username Text NOT NULL,
                            photo_path Text,
                            csv_path Text,
                            created_at Timestamp,
                            PRIMARY KEY (id, username)
                        )
                    ''')
                    logger.info("Table initialized")
                except Exception as e:
                    logger.error(f"Table init error: {str(e)}")
                    raise

            ydb.pool.retry_operation_sync(init_table)

    except Exception as e:
        logger.critical(f"Startup failed: {str(e)}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    if scheduler:
        scheduler.shutdown()
    if ydb and ydb.driver:
        ydb.driver.stop()

@app.get("/health")
async def health_check():
    try:
        if Config.LOCAL_DEBUG:
            with get_sqlite_conn() as conn:
                conn.execute("SELECT 1")
            return {
                "status": "OK",
                "storage_ready": True,
                "db_ready": True,
                "timestamp": datetime.now().isoformat()
            }
        else:
            def check_connection(session):
                session.transaction().execute("SELECT 1")
            
            ydb.pool.retry_operation_sync(check_connection)
            
            return {
                "status": "OK",
                "ydb_ready": True,
                "token_expires": token_manager.expires_at.isoformat(),
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(500, "Service unavailable")

@app.get("/token_info")
async def get_token_info():
    if Config.LOCAL_DEBUG:
        return {
            "token_valid": False,
            "expires_at": None,
            "minutes_remaining": 0
        }
    return {
        "token_valid": token_manager.iam_token is not None,
        "expires_at": token_manager.expires_at.isoformat() if token_manager.expires_at else None,
        "minutes_remaining": (token_manager.expires_at - datetime.utcnow()).total_seconds() / 60 if token_manager.expires_at else 0
    }


@app.post("/users")
async def register_user(payload: UserCreate):
    username = normalize_username(payload.username)
    if not username:
        raise HTTPException(400, "Username is required")

    client_id = f"{username}_{uuid.uuid4()}"

    try:
        if Config.LOCAL_DEBUG:
            with get_sqlite_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO client_data (id, username, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (client_id, username, datetime.utcnow().isoformat())
                )

            return {
                "status": "success",
                "client_id": client_id,
                "username": username
            }
        else:
            if not ydb.driver or not ydb.pool:
                raise HTTPException(500, "YDB connection not initialized")

            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            def execute_query(session):
                query = f"""
                --!syntax_v1
                UPSERT INTO `{full_table_path}`
                (id, username, created_at)
                VALUES (
                    '{client_id}',
                    '{username.replace("'", "''")}',
                    CurrentUtcTimestamp()
                )
                """
                session.transaction().execute(query, commit_tx=True)

            ydb.pool.retry_operation_sync(execute_query)

            return {
                "status": "success",
                "client_id": client_id,
                "username": username
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User registration failed: {str(e)}", exc_info=True)
        raise HTTPException(500, "User registration failed")


@app.get("/users")
async def list_users():
    try:
        if Config.LOCAL_DEBUG:
            with get_sqlite_conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT username, MAX(created_at) AS last_seen
                    FROM client_data
                    WHERE username IS NOT NULL AND username != ''
                    GROUP BY username
                    ORDER BY last_seen DESC
                    """
                )
                rows = cursor.fetchall()

            users = [
                {
                    "username": row[0],
                    "last_seen": serialize_last_seen(row[1])
                }
                for row in rows
            ]
        else:
            if not ydb.driver or not ydb.pool:
                raise HTTPException(500, "YDB connection not initialized")

            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            def execute_query(session):
                query = f"""
                --!syntax_v1
                SELECT
                    username,
                    MAX(created_at) AS last_seen
                FROM `{full_table_path}`
                WHERE username != ""
                GROUP BY username
                ORDER BY last_seen DESC
                """
                result = session.transaction().execute(query, commit_tx=True)
                return result[0].rows or []

            rows = ydb.pool.retry_operation_sync(execute_query)
            users = [
                {
                    "username": row.get("username"),
                    "last_seen": serialize_last_seen(row.get("last_seen"))
                }
                for row in rows
            ]

        return {
            "status": "success",
            "users": users,
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User list failed: {str(e)}", exc_info=True)
        raise HTTPException(500, "User list failed")


@app.get("/get_stats")
async def get_stats():
    try:
        if Config.LOCAL_DEBUG:
            with get_sqlite_conn() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM client_data")
                total_count = cursor.fetchone()[0]
            return {
                "status": "success",
                "table": "client_data",
                "stats": {"total_count": total_count},
                "timestamp": datetime.now().isoformat()
            }
        else:
            logger.debug("Checking YDB driver status...")
            if not ydb.driver or not ydb.pool:
                raise HTTPException(500, "YDB connection not initialized")

            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            def execute_queries(session):
                try:
                    try:
                        session.describe_table(full_table_path)
                        logger.debug(f"Table {full_table_path} exists")
                    except issues.SchemeError as e:
                        logger.error(f"Table error: {str(e)}")
                        raise HTTPException(404, f"Table {full_table_path} not found") from e

                    query = f"""
                    SELECT 
                        COUNT(*) as total_count
                    FROM `{full_table_path}`
                    """
                    
                    result = session.transaction().execute(
                        query,
                        commit_tx=True
                    )
                    
                    return {
                        "total_count": result[0].rows[0].get("total_count", 0)
                    }
                    
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(f"Query error: {str(e)}", exc_info=True)
                    raise HTTPException(500, "Query execution failed") from e

            stats = ydb.pool.retry_operation_sync(execute_queries)
            
            return {
                "status": "success",
                "table": full_table_path,
                "stats": stats,
                "timestamp": datetime.now().isoformat()
            }
        
    except HTTPException as he:
        raise
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(500, "Internal server error")
    

@app.post("/upload_data")
async def upload_data(
    photo: UploadFile,
    csv_file: UploadFile,
    username: str = Form(...)
):
    print('upload_data succsess')
    client_id = username+str(uuid.uuid4())
    
    try:
        if not photo.filename.lower().endswith(('.jpg', '.jpeg', '.png')):
            raise HTTPException(400, "Invalid photo format")
        if not csv_file.filename.lower().endswith('.csv'):
            raise HTTPException(400, "Invalid CSV file")

        photo_ext = Path(photo.filename).suffix
        photo_path = f"photos/{client_id}{photo_ext}"
        csv_path = f"csv/{client_id}.csv"

        if Config.LOCAL_DEBUG:
            base_dir = Path(Config.LOCAL_STORAGE_DIR)
            save_upload_file(photo, base_dir / photo_path)
            save_upload_file(csv_file, base_dir / csv_path)

            with get_sqlite_conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO client_data
                    (id, username, photo_path, csv_path, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (client_id, username, photo_path, csv_path, datetime.utcnow().isoformat())
                )

            return {
                "status": "success",
                "client_id": client_id,
                "photo_url": str(base_dir / photo_path),
                "csv_url": str(base_dir / csv_path)
            }
        else:
            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            async with aioboto3.Session().client(
                's3',
                endpoint_url=Config.S3_ENDPOINT,
                aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
            ) as s3:
                await s3.upload_fileobj(photo.file, Config.BUCKET_NAME, photo_path)
                await s3.upload_fileobj(csv_file.file, Config.BUCKET_NAME, csv_path)

            def execute_query(session):
                try:
                    query = f"""
                    --!syntax_v1
                    UPSERT INTO `{full_table_path}` 
                    (id, username, photo_path, csv_path, created_at)
                    VALUES (
                        '{client_id}',
                        '{username.replace("'", "''")}',
                        '{photo_path.replace("'", "''")}',
                        '{csv_path.replace("'", "''")}',
                        CurrentUtcTimestamp()
                    )
                    """
                    logger.debug(f"Executing YDB query: {query}")
                    session.transaction().execute(
                        query,
                        commit_tx=True
                    )
                except Exception as e:
                    logger.error(f"YDB error: {str(e)}", exc_info=True)
                    raise HTTPException(500, "Database operation failed")

            ydb.pool.retry_operation_sync(execute_query)
            
            return {
                "status": "success",
                "client_id": client_id,
                "photo_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{photo_path}",
                "csv_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{csv_path}"
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        raise HTTPException(500, "Upload operation failed")


@app.post("/upload_video_data")
async def upload_video_data(
    video: UploadFile,
    trajectory: UploadFile,
    username: str = Form(...)
):
    client_id = username + "_" + str(uuid.uuid4())

    try:
        if not video.filename.lower().endswith(('.mp4', '.webm')):
            raise HTTPException(400, "Invalid video format")

        if not trajectory.filename.lower().endswith('.json'):
            raise HTTPException(400, "Invalid trajectory format")

        video_path = f"videos/{client_id}{Path(video.filename).suffix}"
        trajectory_path = f"trajectories/{client_id}.json"

        if Config.LOCAL_DEBUG:
            base_dir = Path(Config.LOCAL_STORAGE_DIR)
            save_upload_file(video, base_dir / video_path)
            save_upload_file(trajectory, base_dir / trajectory_path)

            with get_sqlite_conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO client_data
                    (id, username, video_path, trajectory_path, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (client_id, username, video_path, trajectory_path, datetime.utcnow().isoformat())
                )

            return {
                "status": "success",
                "client_id": client_id,
                "video_url": str(base_dir / video_path),
                "trajectory_url": str(base_dir / trajectory_path)
            }
        else:
            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            async with aioboto3.Session().client(
                's3',
                endpoint_url=Config.S3_ENDPOINT,
                aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
            ) as s3:
                await s3.upload_fileobj(video.file, Config.BUCKET_NAME, video_path)
                await s3.upload_fileobj(trajectory.file, Config.BUCKET_NAME, trajectory_path)

            # ---- запись в YDB ----
            def execute_query(session):
                query = f"""
                --!syntax_v1
                UPSERT INTO `{full_table_path}`
                (id, username, video_path, trajectory_path, created_at)
                VALUES (
                    '{client_id}',
                    '{username.replace("'", "''")}',
                    '{video_path}',
                    '{trajectory_path}',
                    CurrentUtcTimestamp()
                )
                """
                session.transaction().execute(query, commit_tx=True)

            ydb.pool.retry_operation_sync(execute_query)

            return {
                "status": "success",
                "client_id": client_id,
                "video_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{video_path}",
                "trajectory_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{trajectory_path}"
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Video upload failed: {str(e)}", exc_info=True)
        raise HTTPException(500, "Video upload failed")


@app.get("/get_exist_client")
async def get_exist_client(username: str):
    try:
        if Config.LOCAL_DEBUG:
            with get_sqlite_conn() as conn:
                cursor = conn.execute(
                    """
                    SELECT id, username, photo_path, csv_path, created_at
                    FROM client_data
                    WHERE username = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (username,)
                )
                row = cursor.fetchone()

            if row:
                client_data = {
                    "id": row[0],
                    "username": row[1],
                    "photo_url": str(Path(Config.LOCAL_STORAGE_DIR) / row[2]) if row[2] else None,
                    "csv_url": str(Path(Config.LOCAL_STORAGE_DIR) / row[3]) if row[3] else None,
                    "created_at": row[4]
                }
                result = {"exists": True, "client_data": client_data}
            else:
                result = {"exists": False}

            return {
                "status": "success",
                "client_exists": result["exists"],
                "data": result.get("client_data"),
                "timestamp": datetime.now().isoformat()
            }
        else:
            logger.debug(f"Checking client existence for username: {username}")
            if not ydb.driver or not ydb.pool:
                raise HTTPException(500, "YDB connection not initialized")

            full_table_path = f"{Config.YDB_DATABASE}/client_data"

            def execute_queries(session):
                try:
                    try:
                        session.describe_table(full_table_path)
                        logger.debug(f"Table {full_table_path} exists")
                    except issues.SchemeError as e:
                        logger.error(f"Table error: {str(e)}")
                        raise HTTPException(404, f"Table {full_table_path} not found") from e

                    query = f"""
                    --!syntax_v1
                    DECLARE $username AS Text;
                    
                    SELECT 
                        id,
                        username,
                        photo_path,
                        csv_path,
                        created_at
                    FROM `{full_table_path}`
                    WHERE username = $username
                    LIMIT 1
                    """
                    
                    params = {
                        '$username': username
                    }
                    
                    result = session.transaction().execute(
                        query,
                        commit_tx=True,
                        parameters=params
                    )
                    
                    if result[0].rows:
                        client_data = result[0].rows[0]
                        return {
                            "exists": True,
                            "client_data": {
                                "id": client_data.get("id"),
                                "username": client_data.get("username"),
                                "photo_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{client_data.get('photo_path')}",
                                "csv_url": f"{Config.S3_ENDPOINT}/{Config.BUCKET_NAME}/{client_data.get('csv_path')}",
                                "created_at": client_data.get("created_at")
                            }
                        }
                    return {"exists": False}
                    
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(f"Query error: {str(e)}", exc_info=True)
                    raise HTTPException(500, "Query execution failed") from e

            result = ydb.pool.retry_operation_sync(execute_queries)
            
            return {
                "status": "success",
                "client_exists": result["exists"],
                "data": result.get("client_data"),
                "timestamp": datetime.now().isoformat()
            }
        
    except HTTPException as he:
        raise
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(500, "Internal server error")