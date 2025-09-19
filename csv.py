import os
import logging
from pathlib import Path
from typing import Dict, List, Union

logger = logging.getLogger(__name__)

def create_directory(date: str, base_dir: str = "data") -> Path:
    """
    Cria diretório para armazenar dados de uma data específica
    
    Args:
        date: Data no formato YYYY-MM-DD
        base_dir: Diretório base (padrão: 'data')
        
    Returns:
        Path: Caminho do diretório criado
    """
    dir_path = Path(base_dir) / date
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path

def save_single_csv(data: Union[bytes, str], filepath: Path) -> bool:
    """
    Salva dados em um arquivo CSV
    
    Args:
        data: Dados a serem salvos (bytes ou string)
        filepath: Caminho completo do arquivo
        
    Returns:
        bool: True se salvou com sucesso
    """
    try:
        if isinstance(data, bytes):
            with open(filepath, 'wb') as f:
                f.write(data)
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(data)
        
        file_size = filepath.stat().st_size
        logger.info(f"Salvo: {filepath.name} ({file_size / (1024*1024):.1f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar {filepath}: {e}")
        return False

def save_date_results(date_results: List[Dict], base_dir: str = "data") -> Dict:
    """
    Salva todos os resultados de uma data específica
    
    Args:
        date_results: Lista de resultados de scrapers para uma data
        base_dir: Diretório base para salvar
        
    Returns:
        Dict: Estatísticas de salvamento
    """
    if not date_results:
        return {"saved": 0, "errors": 0, "date": "unknown"}
    
    # Pegar a data do primeiro resultado
    date = date_results[0].get("date", "unknown")
    logger.info(f"Salvando dados para {date}")
    
    # Criar diretório para a data
    date_dir = create_directory(date, base_dir)
    
    saved_count = 0
    error_count = 0
    
    for result in date_results:
        if result.get("status") != "success":
            error_count += 1
            continue
            
        try:
            scraper_name = result.get("scraper", "unknown")
            filename = result.get("filename", scraper_name.lower().replace(" ", "_"))
            data = result.get("data")
            
            if not data:
                logger.warning(f"⚠️  Dados vazios para {scraper_name}")
                error_count += 1
                continue
            
            # Criar nome do arquivo
            csv_filename = f"{filename}-{date}.csv"
            filepath = date_dir / csv_filename
            
            # Salvar arquivo
            if save_single_csv(data, filepath):
                saved_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            logger.error(f"Erro ao processar resultado: {e}")
            error_count += 1
    
    logger.info(f"📊 Data {date}: {saved_count} salvos, {error_count} erros")
    
    return {
        "date": date,
        "saved": saved_count,
        "errors": error_count,
        "directory": str(date_dir)
    }

def save_to_csv(scraper_results: Dict, base_dir: str = "data") -> Dict:
    """
    Função principal para salvar todos os resultados do scraper
    
    Args:
        scraper_results: Resultados completos do handler_available_dates_async
        base_dir: Diretório base para salvar (padrão: 'data')
        
    Returns:
        Dict: Estatísticas completas de salvamento
    """
    logger.info(f"Iniciando salvamento de dados em: {base_dir}")
    
    if scraper_results.get("status") != "completed":
        logger.error(f"Scraper não completou com sucesso: {scraper_results.get('status')}")
        return {"status": "error", "message": "Scraper não completou"}
    
    results_list = scraper_results.get("results", [])
    if not results_list:
        logger.warning("Nenhum resultado para salvar")
        return {"status": "no_data", "dates_processed": 0}
    
    # Processar cada data
    all_stats = []
    total_saved = 0
    total_errors = 0
    
    for date_results in results_list:
        stats = save_date_results(date_results, base_dir)
        all_stats.append(stats)
        total_saved += stats["saved"]
        total_errors += stats["errors"]
    
    # Resumo final
    dates_processed = len(all_stats)
    logger.info(f"Salvamento concluído!")
    logger.info(f"Datas processadas: {dates_processed}")
    logger.info(f"Total de arquivos salvos: {total_saved}")
    logger.info(f"Total de erros: {total_errors}")
    
    # Listar diretórios criados
    created_dirs = [stat["directory"] for stat in all_stats]
    logger.info(f"Diretórios criados: {len(created_dirs)}")
    
    return {
        "status": "completed",
        "dates_processed": dates_processed,
        "total_saved": total_saved,
        "total_errors": total_errors,
        "directories": created_dirs,
        "details": all_stats
    }

def save_with_custom_mapping(scraper_results: Dict, filename_mapping: Dict[str, str], base_dir: str = "data") -> Dict:
    """
    Salva dados com mapeamento customizado de nomes de arquivos
    
    Args:
        scraper_results: Resultados do scraper
        filename_mapping: Mapeamento de nomes {"Scraper Name": "custom_filename"}
        base_dir: Diretório base
        
    Returns:
        Dict: Estatísticas de salvamento
    """
    logger.info(f"Salvando com mapeamento customizado: {filename_mapping}")
    
    # Temporariamente aplicar mapeamento
    results_list = scraper_results.get("results", [])
    for date_results in results_list:
        for result in date_results:
            scraper_name = result.get("scraper")
            if scraper_name in filename_mapping:
                result["filename"] = filename_mapping[scraper_name]
    
    return save_to_csv(scraper_results, base_dir)

# Funções utilitárias

def list_saved_files(base_dir: str = "data") -> Dict:
    """Lista todos os arquivos salvos organizados por data"""
    base_path = Path(base_dir)
    
    if not base_path.exists():
        return {"directories": 0, "files": 0, "dates": []}
    
    dates_info = []
    total_files = 0
    
    for date_dir in base_path.iterdir():
        if date_dir.is_dir():
            files = list(date_dir.glob("*.csv"))
            total_size = sum(f.stat().st_size for f in files)
            
            dates_info.append({
                "date": date_dir.name,
                "files": len(files),
                "total_size_mb": round(total_size / (1024*1024), 1),
                "filenames": [f.name for f in files]
            })
            total_files += len(files)
    
    return {
        "directories": len(dates_info),
        "total_files": total_files,
        "dates": sorted(dates_info, key=lambda x: x["date"], reverse=True)
    }

def cleanup_old_files(base_dir: str = "data", keep_days: int = 30) -> Dict:
    """Remove arquivos mais antigos que X dias"""
    from datetime import datetime, timedelta
    
    base_path = Path(base_dir)
    if not base_path.exists():
        return {"removed": 0, "kept": 0}
    
    cutoff_date = datetime.now() - timedelta(days=keep_days)
    removed = 0
    kept = 0
    
    for date_dir in base_path.iterdir():
        if date_dir.is_dir():
            try:
                dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
                if dir_date < cutoff_date:
                    import shutil
                    shutil.rmtree(date_dir)
                    logger.info(f"Removido diretório antigo: {date_dir.name}")
                    removed += 1
                else:
                    kept += 1
            except ValueError:
                # Nome de diretório inválido, pular
                continue
    
    return {"removed": removed, "kept": kept}

# Exemplo de uso
if __name__ == "__main__":
    # Exemplo de como usar as funções
    
    # Simulação de resultados do scraper
    example_results = {
        "status": "completed",
        "results": [
            [
                {
                    "scraper": "Open Interest",
                    "date": "2024-09-18",
                    "status": "success",
                    "data": b"ticker,price,volume\nPETR4,32.50,1000\n",
                    "error": None
                }
            ]
        ]
    }
    
    # Salvar dados
    stats = save_to_csv(example_results)
    print(stats)
    
    # Listar arquivos salvos
    files_info = list_saved_files()
    print(files_info)