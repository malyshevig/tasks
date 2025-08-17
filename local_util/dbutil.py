import logging
import threading as th

import psycopg2
from psycopg2 import pool


host = "localhost"
port = 5432

user = "postgres"
password = "begemot"

class DbUtil:
    def __init__(self, dbname:str):
        self.dbname = dbname

        print (f"init db util {dbname} ")

        self. pool = psycopg2.pool.SimpleConnectionPool(
                minconn=10,  # Минимальное количество соединений
                maxconn=10,   # Максимальное количество соединений
                host = host, port = port, dbname=self.dbname, user = user, password=password
            )

    def __del__(self):
        if self.pool:
            self.pool.closeall()

    def query_template(self, callback):
        cur = None

        with self.pool.getconn() as conn:
            logging.info(f"get connection {conn} {th.current_thread()}")
            if not conn:
                raise Exception("Cant get connection")

            try:
                cur = conn.cursor()
                r = callback(conn, cur)

                return r
            finally:
                if cur:
                    cur.close()
                logging.info(f"put connection {conn} {th.current_thread()}")
                self.pool.putconn(conn)

    def execute_query_update(self, query) -> int:

        logging.debug(f"Execute query update: {query}")
        def call (conn, cur):
            cur.execute(query)
            c = cur.rowcount
            conn.commit()
            return c

        return self.query_template (call)

    def execute_query_select (self, query, limit=1):

        logging.debug(f"Execute query select: {query}")
        def call (_, cur):
            cur.execute(query)
            data = cur.fetchall ()
            return data[:limit] if limit else data

        return self.query_template(call)

    def update_outdated_tasks(self)-> int:
        c = 0
        q1 = "update task set status='open', fail_count = fail_count+1 "
        q1+= "where status = 'in_progress' "
        q1+= "and ts <= (NOW() - INTERVAL '10 SECONDS') "
        q1+= "and fail_count<2;"
        c = c + self.execute_query_update (q1)

        q2 = "update task set status='failed', fail_count = fail_count+1 "
        q2+= "where status = 'in_progress' "
        q2+= "and ts <= (NOW() - INTERVAL '10 SECONDS') "
        q2+= "and fail_count>=2;"

        c = c + self.execute_query_update (q2)
        return c
