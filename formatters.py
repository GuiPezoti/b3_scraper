
def earnings_formatter(content_b: bytes):
    earnings_csv = content_b
    earnings_csv = str(content_b, 'utf-8')[2:-1]
    earnings_csv = earnings_csv.replace('\\t', ',').replace('\\n', '\n')
    earnings_csv = bytes(earnings_csv.split('\n\n')[1].encode('utf-8'))
    return earnings_csv
