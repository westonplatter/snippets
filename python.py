from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import List, Dict

def rows_as_dicts(cursor):
    """
    Convert tuple result to dict with cursor
    """
    col_names = [i[0] for i in cursor.description]
    return [dict(zip(col_names, row)) for row in cursor]

def execute_raw_sql(session: Session, sql: str) -> List[Dict]:
    """execute raw sql and get result as list of dictionaries

    Args:
        session (Session): _description_
        sql (str): _description_

    Returns:
        list: _description_
    """
    cursor = session.execute(text(sql)).cursor
    return rows_as_dicts(cursor)

class PnlService: 
    def fetch_pnl_by_grouping(session: Session):
        sql = """
            with grouping_pnl as (
                select 
                    tg.grouping_id as grouping_id
                    , sum(t.fifo_pnl_realized) as pnl_realized
                from trade_groupings tg 
                join trades t on t.trade_id = tg.trade_id
                join groupings g on tg.grouping_id = g.id
                group by tg.grouping_id
            )
            select gpnl.*, g.*, s.id as sid, s.key as skey, s.description as sdesc
            from grouping_pnl as gpnl
            join groupings as g on g.id = gpnl.grouping_id
            join strategies as s on s.id = g.strategy_id
            ;
        """
        return execute_raw_sql(session, sql)
