import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from scrapers import fetch_earnings, fetch_daily_trades, fetch_open_interest, fetch_series, fetch_consolidated_trade_info, available_dates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

holidays = ["2024-01-01", "2024-02-12", "2024-02-13", "2024-03-29", "2024-05-01", "2024-05-30", "2024-11-15", "2024-11-20", "2024-12-24", "2024-12-25", "2024-12-31"]
scrapers = [
    {"name": "Series", "scraper": fetch_series, "bucket_name": "series-csvs", "filename": "series"},
    {"name": "Earnings", "scraper": fetch_earnings, "bucket_name": "earnings-csvs", "filename": "earnings"},
    {"name": "Open interest", "scraper": fetch_open_interest, "bucket_name": "openinterests-csvs", "filename": "open_interest"},
    {"name": "Consolidated trades", "scraper": fetch_consolidated_trade_info, "bucket_name": "consolidated-csvs", "filename": "consolidated_trades_info"},
    {"name": "Daily trades", "scraper": fetch_daily_trades, "bucket_name": "trades-csvs", "filename": "daily_trades"}
    ]

async def single_scraper(session: aiohttp.ClientSession, scraper: Dict, date: str):
    """Executa um scraper específico para uma data"""
    try: 
        logger.info(f"Executando {scraper['name']} para {date}")
        scraped_data = await scraper["scraper"](session, date)
        logger.info(f"{scraper['name']} concluído para {date}")
        #Scraped data é a informação obtida do site da B3
        return scraped_data
    except Exception as e:
        logger.error(f"Erro em {scraper['name']} para {date}: {e}")
        return {"scraper": scraper["name"], "date": date, "status": "error", "error": str(e)}

async def single_date(session: aiohttp.ClientSession, date: str, scrapers_list: List[Dict]) -> List[Dict]:
    """Executa todos os scrapers para uma data específica em paralelo"""
    logger.info(f"Iniciando scraping para {date}")
    tasks = [single_scraper(session, scraper, date) for scraper in scrapers_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_results = []
    for result in results:
        if isinstance(result, Exception):
            processed_results.append({
                "scraper": "unknown", 
                "date": date, 
                "status": "error", 
                "error": str(result)
            })
        else:
            processed_results.append(result)
    
    logger.info(f"Scraping concluído para {date}")
    return processed_results


async def handler_available_dates_async(max_dates: int = 7) -> Dict:
    """Handler assíncrono para múltiplas datas"""
    logger.info(f"Iniciando scraper para múltiplas datas...")
    
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            dates = await available_dates(session)
            if not dates:
                logger.warning("Nenhuma data disponível")
                return {"status": "no_dates", "results": []}
            
            dates_to_process = dates[:max_dates]
            logger.info(f"Processando {len(dates_to_process)} datas")
            
            tasks = []
            for date_info in dates_to_process:
                date_str = date_info[:10] 
                task = single_date(session, date_str, scrapers)
                tasks.append(task)
            all_results = await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Erro ao obter datas: {e}")
            return {"status": "error", "error": str(e)}
    total_success = sum(sum(1 for r in date_results if r["status"] == "success") for date_results in all_results)
    total_errors = sum(sum(1 for r in date_results if r["status"] == "error") for date_results in all_results)
    
    logger.info(f"Resumo geral: {total_success} sucessos, {total_errors} erros em {len(all_results)} datas")
    
    return {
        "status": "completed",
        "dates_processed": len(all_results),
        "results": all_results,
        "summary": {"total_success": total_success, "total_errors": total_errors}
    }

async def main():
    result = asyncio.run(handler_available_dates_async(max_dates=7))
    logger.info("Execução finalizada")
    return result

if __name__ == "__main__":
    main()