import aiohttp
import asyncio
from datetime import datetime
from typing import Any

import json
from io import BytesIO
from zipfile import ZipFile

from formatters import earnings_formatter

async def fetch_earnings(session: aiohttp.ClientSession, date: str) -> Any:
    data = {"Name":"ProventionCreditVariable", "Date":date, "FinalDate":date, "ClientId":"", "Filters":{}}
    async with session.post("https://arquivos.b3.com.br/bdi/table/export/csv?sort=TckrSymb&lang=pt-BR", json=data) as response:
        content = await response.read()
        earnings_info = earnings_formatter(content)
        return earnings_info

async def fetch_daily_trades(session: aiohttp.ClientSession, date: str) -> Any:
    async with session.get(f"https://arquivos.b3.com.br/rapinegocios/tickercsv/{date}", stream=True) as response:
        content = await response.read()
        with ZipFile(BytesIO(content)) as thezip:
            for name in thezip.namelist():
                return thezip.read(name)
        
async def fetch_open_interest(session: aiohttp.ClientSession, date: str) -> Any:
    #Open Interest is alwais D-1, the rest can be fetched on D-0.
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=DerivativesOpenPositionFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        content = await response.read()
        open_interest = content.decode('utf-8').replace('\t', ',').replace('\r', '')
        return open_interest

async def fetch_series(session: aiohttp.ClientSession, date: str) -> Any:
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidatedFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        series = await response.read()
        return series

async def fetch_consolidated_trade_info(session: aiohttp.ClientSession, date: str) -> Any:
    #Open Interest is alwais D-1, the rest can be fetched on D-0.
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=TradeInformationConsolidatedFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        content = await response.read()
        consolidated_trade_info = content.decode('utf-8').replace('\t', ',').replace('\r', '')
        return consolidated_trade_info

async def available_dates(session: aiohttp.ClientSession) -> Any:
    today = datetime.today()
    today = today.strftime('%Y-%m-%d')
    async with session.get(f"https://arquivos.b3.com.br/bdi/table/workdays?date={today}") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        return response_content[1:-1]