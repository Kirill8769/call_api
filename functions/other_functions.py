import hmac
import hashlib
import os

from dotenv import load_dotenv


def get_signature(request_data: str) -> str:
    """
    Создаем объект hmac с использованием секретного ключа и алгоритма SHA256
    Добавляем данные запроса
    Получаем подпись запроса
    """

    load_dotenv()
    hash_data = hmac.new(os.getenv('SECRET_KEY').encode(), digestmod=hashlib.sha256)
    hash_data.update(request_data.encode())
    signature = hash_data.hexdigest()
    return signature
