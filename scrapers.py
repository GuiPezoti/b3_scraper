import aiohttp
import asyncio
from datetime import datetime
from typing import Any

import json
from io import BytesIO
from zipfile import ZipFile

from formatters import earnings_formatter

async def fetch_earnings(session: aiohttp.ClientSession, date: str) -> Any:
    """
    Busca os dados de proventos na B3 para uma data específica.

    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.
        date (str): A data no formato 'YYYY-MM-DD' para a qual buscar os dados.

    Returns:
        Any: Os dados de proventos formatados, conforme processado por `earnings_formatter`.
    """
    data = {"Name":"ProventionCreditVariable", "Date":date, "FinalDate":date, "ClientId":"", "Filters":{}}
    async with session.post("https://arquivos.b3.com.br/bdi/table/export/csv?sort=TckrSymb&lang=pt-BR", json=data) as response:
        content = await response.read()
        earnings_info = earnings_formatter(content)
        return earnings_info

async def fetch_daily_trades(session: aiohttp.ClientSession, date: str) -> Any:
    """
    Baixa os dados de negociação diária (trades) da B3 para uma data específica.

    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.
        date (str): A data no formato 'YYYY-MM-DD' para a qual buscar os dados.

    Returns:
        Any: O conteúdo do arquivo de negociações em formato binário.
    """
    timeout = aiohttp.ClientTimeout(total=600, sock_read=60)
    async with session.get(f"https://arquivos.b3.com.br/rapinegocios/tickercsv/{date}", timeout=timeout) as response:
        buffer = BytesIO()
        async for chunk in response.content.iter_chunked(8192):
            buffer.write(chunk)
        buffer.seek(0)
        with ZipFile(buffer) as thezip:
            return thezip.read(thezip.namelist()[0])
        
async def fetch_open_interest(session: aiohttp.ClientSession, date: str) -> Any:
    """
    Busca o 'open interest' (posições em aberto) de derivativos na B3.
    É importante notar que o 'open interest' é sempre para D-1.

    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.
        date (str): A data no formato 'YYYY-MM-DD' para a qual buscar os dados.

    Returns:
        Any: O conteúdo do arquivo de 'open interest' como uma string formatada.
    """
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=DerivativesOpenPositionFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        content = await response.read()
        open_interest = content.decode('utf-8').replace('\t', ',').replace('\r', '')
        return open_interest

async def fetch_series(session: aiohttp.ClientSession, date: str) -> Any:
    """
    Busca informações consolidadas de séries de instrumentos (ativos) na B3.

    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.
        date (str): A data no formato 'YYYY-MM-DD' para a qual buscar os dados.

    Returns:
        Any: O conteúdo do arquivo de séries em formato binário.
    """
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=InstrumentsConsolidatedFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        series = await response.read()
        return series

async def fetch_consolidated_trade_info(session: aiohttp.ClientSession, date: str) -> Any:
    """
    Busca informações de negociação consolidadas na B3 para uma data específica.

    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.
        date (str): A data no formato 'YYYY-MM-DD' para a qual buscar os dados.

    Returns:
        Any: O conteúdo do arquivo de informações de negociação consolidadas.
    """
    async with session.get(f"https://arquivos.b3.com.br/api/download/requestname?fileName=TradeInformationConsolidatedFile&date={date}&recaptchaToken=") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        token = response_content["token"]
    async with session.get(f"https://arquivos.b3.com.br/api/download/?token={token}") as response:
        content = await response.read()
        consolidated_trade_info = content.decode('utf-8').replace('\t', ',').replace('\r', '')
        return consolidated_trade_info

async def available_dates(session: aiohttp.ClientSession) -> Any:
    """
    Busca as datas de dias úteis disponíveis na B3.
    
    Args:
        session (aiohttp.ClientSession): A sessão aiohttp para a requisição.

    Returns:
        Any: Uma lista com as datas úteis.
    """
    today = datetime.today()
    today = today.strftime('%Y-%m-%d')
    async with session.get(f"https://arquivos.b3.com.br/bdi/table/workdays?date={today}") as response:
        content = await response.read()
        response_content = json.loads(content.decode())
        return response_content[1:-1]