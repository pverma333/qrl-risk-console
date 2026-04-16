from typing import Dict, List, Any
from datetime import date
import duckdb


def get_market_summary(conn: duckdb.DuckDBPyConnection, trade_date: date) -> Dict[str, Any]:
    """
    Fetch market summary for given trade date.

    Returns:
        - indices: list of 4 index dicts with OHLC and change
        - vix: dict with current value
        - yields: dict with 3M/6M/1Y rates
        - chart_data: list of dicts for 30-day closing prices
    """

    # 1) Index OHLC for the given trade date
    index_query = """
    SELECT
        symbol,
        open,
        high,
        low,
        close
    FROM v_processed_index_spot
    WHERE trade_date = ?
      AND symbol IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
    ORDER BY CASE symbol
        WHEN 'NIFTY' THEN 1
        WHEN 'BANKNIFTY' THEN 2
        WHEN 'FINNIFTY' THEN 3
        WHEN 'MIDCPNIFTY' THEN 4
    END
    """
    current_indices = conn.execute(index_query, [trade_date]).fetchall()

    if not current_indices:
        raise ValueError(
            f"No index data found for trade_date={trade_date}. "
            f"Date may be a holiday or outside the data range."
        )

    # 2) Previous day close for change calculation
    prev_query = """
    WITH prev_date AS (
        SELECT MAX(trade_date) as prev_td
        FROM v_processed_index_spot
        WHERE trade_date < ?
    )
    SELECT
        symbol,
        close as prev_close
    FROM v_processed_index_spot
    WHERE trade_date = (SELECT prev_td FROM prev_date)
      AND symbol IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
    """
    prev_indices = {
        row[0]: row[1]
        for row in conn.execute(prev_query, [trade_date]).fetchall()
    }

    # 3) Build index list with change
    symbol_map = {
        'NIFTY': 'Nifty 50',
        'BANKNIFTY': 'Bank Nifty',
        'FINNIFTY': 'Fin Nifty',
        'MIDCPNIFTY': 'Midcap Nifty'
    }

    indices = []
    for symbol, open_val, high_val, low_val, close_val in current_indices:
        prev_close = prev_indices.get(symbol, close_val)
        change = close_val - prev_close
        change_pct = (change / prev_close) * 100 if prev_close else 0.0

        indices.append({
            'symbol': symbol,
            'display_name': symbol_map.get(symbol, symbol),
            'open': round(open_val, 2),
            'high': round(high_val, 2),
            'low': round(low_val, 2),
            'close': round(close_val, 2),
            'change': round(change, 2),
            'change_pct': round(change_pct, 2)
        })

    # 4) VIX
    vix_query = """
    SELECT close as vix_value
    FROM v_processed_vix
    WHERE trade_date = ?
    """
    vix_row = conn.execute(vix_query, [trade_date]).fetchone()
    vix = {'value': round(vix_row[0], 2) if vix_row else None}

    # 5) G-Bond yields
    # tenor column is stored lowercase (3m, 6m, 1y) — use UPPER() for safe matching
    yield_query = """
    SELECT
        MAX(CASE WHEN UPPER(tenor) = '3M' THEN yield_pct END) as rate_3m,
        MAX(CASE WHEN UPPER(tenor) = '6M' THEN yield_pct END) as rate_6m,
        MAX(CASE WHEN UPPER(tenor) = '1Y' THEN yield_pct END) as rate_1y
    FROM v_processed_gbond
    WHERE trade_date = ?
      AND UPPER(tenor) IN ('3M', '6M', '1Y')
    """
    yield_row = conn.execute(yield_query, [trade_date]).fetchone()
    yields = {
        '3M': round(yield_row[0], 2) if yield_row and yield_row[0] else None,
        '6M': round(yield_row[1], 2) if yield_row and yield_row[1] else None,
        '1Y': round(yield_row[2], 2) if yield_row and yield_row[2] else None
    }

    # 6) 30-day chart data
    chart_query = """
    WITH last_30_dates AS (
        SELECT DISTINCT trade_date
        FROM v_processed_index_spot
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT 30
    )
    SELECT
        i.trade_date,
        i.symbol,
        i.close
    FROM v_processed_index_spot i
    INNER JOIN last_30_dates d ON i.trade_date = d.trade_date
    WHERE i.symbol IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
    ORDER BY i.trade_date ASC, i.symbol
    """
    chart_rows = conn.execute(chart_query, [trade_date]).fetchall()

    chart_data = [
        {
            'date': str(row[0]),
            'symbol': row[1],
            'close': round(row[2], 2)
        }
        for row in chart_rows
    ]

    return {
        'indices': indices,
        'vix': vix,
        'yields': yields,
        'chart_data': chart_data
    }
