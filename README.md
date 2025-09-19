# B3 Async Scraper

Scraper assíncrono para coleta de dados públicos da B3 (Brasil Bolsa Balcão). O código trabalha apenas com alguns dos endpoints disponíveis e pode ser expandido. Novos scrapers podem ser adicionados a execução. 
## Instalação

```bash
pip install aiohttp asyncio
```

## Estrutura do Projeto

```
├── main.py           # Script principal assíncrono
├── scrapers.py       # Funções de scraping para cada endpoint
├── formatters.py     # Formatadores de dados
├── csv_saver.py      # Funções para salvar CSVs localmente
└── aws_s3.py         # Funções de upload S3 (opcional)
```

## Dados Coletados

O scraper coleta os seguintes tipos de dados da B3:

| Scraper | Descrição | Endpoint |
|---------|-----------|----------|
| **Series** | Instrumentos consolidados | InstrumentsConsolidatedFile |
| **Earnings** | Proventos e dividendos | ProventionCreditVariable |
| **Open Interest** | Posições em aberto (D-1) | DerivativesOpenPositionFile |
| **Consolidated Trades** | Negociações consolidadas | TradeInformationConsolidatedFile |
| **Daily Trades** | Negociações diárias | tickercsv |

## Como Usar

### Execução Básica
```bash
python main.py
```

Os dados serão automaticamente salvos na pasta `data/` organizados por data.

### Configuração de Datas
```python
# No main.py, modificar o parâmetro max_dates:
result = await handler_available_dates_async(max_dates=7)  # Últimas 7 datas
```

### Habilitando/Desabilitando Scrapers
```python
# No main.py, comentar scrapers não desejados:
scrapers = [
    {"name": "Series", "scraper": fetch_series, "bucket_name": "series-csvs", "filename": "series"},
    # {"name": "Earnings", "scraper": fetch_earnings, "bucket_name": "earnings-csvs", "filename": "earnings"},  # Desabilitado
    {"name": "Open interest", "scraper": fetch_open_interest, "bucket_name": "openinterests-csvs", "filename": "open_interest"},
]
```

## Salvamento Automático de CSVs

### Estrutura de Diretórios Criada Automaticamente
```
data/
├── 2024-09-18/
│   ├── series-2024-09-18.csv
│   ├── earnings-2024-09-18.csv
│   ├── open_interest-2024-09-18.csv
│   ├── consolidated_trades_info-2024-09-18.csv
│   └── daily_trades-2024-09-18.csv (800MB)
├── 2024-09-17/
│   └── [mesmos arquivos...]
└── 2024-09-16/
    └── [mesmos arquivos...]
```

### Personalizar Diretório de Salvamento
```python
# No main.py, modificar a chamada save_to_csv:
save_stats = save_to_csv(result, base_dir="meus_dados_b3")
```

### Personalizar Nomes dos Arquivos
```python
# Modificar os nomes de arquivo na configuração dos scrapers:
scrapers = [
    {"name": "Open Interest", "scraper": fetch_open_interest, "bucket_name": "openinterests-csvs", "filename": "posicoes_abertas"},
    {"name": "Daily Trades", "scraper": fetch_daily_trades, "bucket_name": "trades-csvs", "filename": "negociacoes_diarias"},
    # Resultado: posicoes_abertas-2024-09-18.csv
]
```

### Gerenciamento de Arquivos
```python
from csv_saver import list_saved_files, cleanup_old_files

# Listar arquivos salvos
files = list_saved_files()
print(f"Total: {files['total_files']} arquivos em {files['directories']} datas")

# Limpar arquivos antigos (manter apenas 30 dias)
cleanup_stats = cleanup_old_files(keep_days=30)
print(f"Removidos: {cleanup_stats['removed']} diretórios antigos")
```

## Configurações Avançadas

### Limites de Conexão
```python
connector = aiohttp.TCPConnector(
    limit=20,        # Máximo de conexões simultâneas
    limit_per_host=10 # Máximo por host
)
```

### Timeouts Otimizados por Tamanho de Arquivo
```python
# Para arquivos grandes (800MB - Daily Trades)
timeout = aiohttp.ClientTimeout(total=600, sock_read=60)  # 10 min

# Para arquivos médios (50-100MB)
timeout = aiohttp.ClientTimeout(total=300, sock_read=30)  # 5 min

# Para arquivos pequenos (<10MB)
timeout = aiohttp.ClientTimeout(total=60)  # 1 min
```


## Estrutura de Retorno

```python
{
    "status": "completed",
    "dates_processed": 7,
    "results": [
        [  # Resultados da data 1
            {
                "scraper": "Series",
                "date": "2024-09-18",
                "status": "success",
                "data": b"dados_csv_series...",
                "error": None
            },
            # ... outros scrapers
        ],
        # ... outras datas
    ],
    "summary": {
        "total_success": 28,
        "total_errors": 7
    }
}
```

## Tratamento de Erros

### Logs Detalhados
- **INFO**: Progresso de cada scraper
- **ERROR**: Erros específicos com detalhes
- **INFO**: Resumo final de sucessos/erros

# Solução de Problemas

### Erro de Timeout (Arquivos Grandes)
```python
# Aumentar timeout para Daily Trades (800MB)
timeout = aiohttp.ClientTimeout(total=900, sock_read=120)  # 15 min
```

### Erro de Memória
```python
# Reduzir paralelismo para arquivos muito grandes
connector = aiohttp.TCPConnector(limit=5, limit_per_host=2)
```

### Erro de Rate Limit
```python
# Adicionar delay entre datas
import asyncio
await asyncio.sleep(2)  # 2 segundos entre processamento de datas
```

### Arquivos Corrompidos
- Verificar conectividade com B3
- Reprocessar datas específicas
- Verificar logs de erro detalhados