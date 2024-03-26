import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def category_hk_take(self,
                        category_name: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_category_pk 
                        FROM dds.h_category
                        WHERE category_name = %(category_name)s;
                    """,
                    {
                        'category_name': category_name,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)

    def product_hk_take(self,
                        product_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_product_pk
                        FROM dds.h_product
                        WHERE product_id = %(product_id)s;
                    """,
                    {
                        'product_id': product_id,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)

    def user_hk_take(self,
                        user_id: str
                        ) -> str:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT h_user_pk
                        FROM dds.h_user
                        WHERE user_id = %(user_id)s;
                    """,
                    {
                        'user_id': user_id,                
                    }
                )
                pk = str(cur.fetchone()[0])
                return(pk)
            
    def user_product_counters_insert(self,
                        user_id: str,
                        product_id: str,
                        product_name: str,
                        order_cnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
						VALUES(%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
						ON CONFLICT (user_id, product_id) DO UPDATE
						SET order_cnt = user_product_counters.order_cnt + EXCLUDED.order_cnt,
						product_name = EXCLUDED.product_name;
                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                        'order_cnt': order_cnt
                    }
                )

    def user_category_counters_insert(self,
                        user_id: str,
                        category_id: str,
                        category_name: str,
                        order_cnt: int
                        ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
						VALUES(%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
						ON CONFLICT (user_id, category_id) DO UPDATE
						SET order_cnt = user_category_counters.order_cnt + EXCLUDED.order_cnt,
						category_name = EXCLUDED.category_name;
                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                        'order_cnt': order_cnt
                    }
                )