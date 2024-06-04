import asyncpg
import logging
import os

from dotenv import load_dotenv


class BitrixDB:
    """
    Класс для работы с базой данных Bitrix.
    """

    def __init__(self) -> None:
        load_dotenv()
        self._connection = None
        self.__host = os.getenv("HOST")
        self.__database = os.getenv("DATABASE")
        self.__user = os.getenv("USER")
        self.__password = os.getenv("PASSWORD")
        logging.info("Database is activated")

    async def connect(self) -> None:
        """
        Устанавливает соединение с базой данных.
        """
        try:
            self._connection = await asyncpg.connect(
                host=self.__host, database=self.__database, user=self.__user, password=self.__password
            )
        except Exception as ex:
            logging.debug(f"{ex.__class__.__name__}: {ex}", exc_info=True)

    async def get_answer_json(self, start_id: int, limit: int = 50):
        """
        Получает JSON-данные из базы данных.

        :param start_id: Начальный идентификатор записи.
        :param limit: Максимальное количество записей для выборки.
        :return: Результат запроса.
        """

        try:
            await self.connect()
            result = await self._connection.fetch('''
                SELECT ID, STAGE, DEAL_URL, TYPE,
                DURATION, MANAGER_ID, DATE, TIMEZONE
                FROM b24_data
                WHERE ID >= $1
                ORDER BY ID
                LIMIT $2
            ''', start_id, limit)
            return result
        except Exception as ex:
            logging.error(f'[!] JSON answer error: {ex}')
        finally:
            await self._connection.close()

    async def get_file_name(self, entry_id: int) -> str:
        """
        Получает имя файла для указанной записи.

        :param entry_id: Идентификатор записи.
        :return: Имя файла.
        """
        try:
            await self.connect()
            file = await self._connection.fetchrow('SELECT FILE_NAME FROM b24_data WHERE ID = $1', entry_id)
            return file['file_name']
        except Exception as ex:
            logging.error(f'[!] File answer error: {ex}')
        finally:
            await self._connection.close()

    async def set_status(self, list_id: list[int]) -> None:
        """
        Устанавливает статус для указанных записей.

        :param list_id: Список идентификаторов записей.
        """
        try:
            await self.connect()
            await self._connection.execute('UPDATE b24_data SET SEND_STATUS = $1 WHERE ID = ANY($2)', '[+]', list_id)
        except Exception as ex:
            logging.error(f'[!] Set status error: {ex}')
        finally:
            await self._connection.close()
