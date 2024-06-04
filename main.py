import os
import logging

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, status, Header, Request
from fastapi.responses import JSONResponse, FileResponse

from functions.db_functions import BitrixDB
from functions.other_functions import get_signature

load_dotenv()
app = FastAPI()
db = BitrixDB()


@app.get('/calls')
async def get_json(start_id: int,
                   req: Request,
                   authorization: str = Header(),
                   signature: str = Header()) -> JSONResponse:
    """
    Получает JSON-данные о звонках из базы данных.

    :param start_id: Начальный идентификатор записи.
    :param req: Запрос FastAPI.
    :param authorization: Заголовок с токеном авторизации.
    :param signature: Заголовок с подписью запроса.
    :return: JSON-ответ с данными о звонках или ошибку авторизации.
    """
    request_data = fr'GET{os.getenv("PORTAL")}/calls?start_id={start_id}'
    my_signature = get_signature(request_data)
    logging.error(fr'[+] INFO: {request_data}')
    if signature == my_signature and authorization == f'Bearer {os.getenv("API_KEY")}':
        result = await db.get_answer_json(start_id)
        if result is not None:
            result_data = [dict(row) for row in result]
            result_list_id = [row['id'] for row in result]
            await db.set_status(list_id=result_list_id)
            logging.error(f'[+] INFO: {len(result_data)}')
            return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content=result_data)
    logging.error(f'[!] ERROR 401. INFO: {req.headers}')
    return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                'error': 'invalid_api_key_or_signature',
                'message': 'API key or signature is invalid or missing.'})


@app.get('/calls/{call_id}')
async def get_file(call_id: int,
                   req: Request,
                   authorization: str = Header(),
                   signature: str = Header()) -> JSONResponse:
    """
    Получает файл записи звонка из базы данных.

    :param call_id: Идентификатор записи звонка.
    :param req: Запрос FastAPI.
    :param authorization: Заголовок с токеном авторизации.
    :param signature: Заголовок с подписью запроса.
    :return: Файл записи звонка или ошибку авторизации или отсутствия записи.
    """
    request_data = fr'GET{os.getenv("PORTAL")}/calls/{call_id}'
    my_signature = get_signature(request_data)
    logging.error(fr'[+] INFO: {request_data}')
    if signature == my_signature and \
       authorization == f'Bearer {os.getenv("API_KEY")}':
        file_name = await db.get_file_name(entry_id=call_id)
        if file_name:
            file_path = os.path.join(os.getenv("FILE_DIRECTORY"), file_name)
            if os.path.exists(file_path):
                logging.error(f'[+] INFO: Return file - {call_id}')
                return FileResponse(
                    path=file_path)
        logging.error(f'[!] ERROR 404. INFO: Call_id - {call_id}')
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND,
                            content={"error": "not_found",
                                     "message": "Call not found"})
    else:
        logging.error(f'[!] ERROR 401. INFO: {req.headers}')
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                'error': 'invalid_api_key_or_signature',
                'message': 'API key or signature is invalid or missing.'})


if __name__ == '__main__':
    logging.basicConfig(
        filename='call_api.log',
        level=logging.ERROR,
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p')
    uvicorn.run(
        app='main:app',
        host='127.0.0.1',
        port=8000,
        reload=True)
