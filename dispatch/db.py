from distutils.util import execute
from shlex import quote

import psycopg2
from psycopg2 import pool

from . local_types import Task

host = "localhost"
port = 5432
dbname = "report"
user = "postgres"
password = "begemot"


def row2task(row):
    return Task(id=row[0], name=row[1], status=row[2], worker_id=row[3], ts=row[4], lines=row[5],
               fail_count=row[6]) if row else None


class DbUtil:
    def __init__(self):
        self. pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,  # Минимальное количество соединений
                maxconn=5,   # Максимальное количество соединений
                host = host, port = port, dbname=dbname, user = user, password=password
            )

    def query_template(self, callback):
        cur = None
        conn = None

        try:
            conn = self.pool.getconn()
            if not conn:
                raise Exception("Cant get connection")

            cur = conn.cursor()
            r = callback(conn, cur)

            return r
        finally:
            if conn:
                if cur:
                    cur.close()
                self.pool.putconn(conn)

    def execute_query_update(self, query) -> int:
        print ("Execute query update: ", query)
        def call (conn, cur):
            cur.execute(query)
            c = cur.rowcount
            conn.commit()
            return c

        return self.query_template (call)

    def execute_query_select (self, query, limit=1):
        print("Execute query select: ", query)
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

    def update_task_status(self, task: Task):
        q_update = f"update task set status = '{task.status}', lines= {task.lines}, ts = now() where id = {task.id};"

        return self.execute_query_update (q_update)

    def get_task (self, task_id)-> Task:
        q = f"select * from task where task.id = {task_id};"

        r = self.execute_query_select (q, limit=1)
        if len(r) > 0:
            row = r[0]

            task = row2task(row)
            return task
        else:
            return None

    def get_tasks(self) -> list:
        q = "select id, name, status, worker_id, ts,lines,fail_count from task;"
        r = self.execute_query_select (q, limit=None)

        return  [row2task(row) for row in r]

    def get_working_task_for_worker(self, worker_id):
        assert worker_id is not None, f"Unexpected parameter value worker_id: {worker_id}"

        q = f"select * from task where worker_id = '{worker_id}' and status = 'in_progress' limit 1;"
        r = self.execute_query_select(q, 1)

        return row2task(r[0]) if len(r) > 0 else None

    def lock_task(self, worker_id) -> Task:
        assert worker_id is not None, f"Unexpected parameter value worker_id: {worker_id}"

        q = f"select * from task where status = 'open' order by id limit 1;"
        while True:
            task: Task

            r = self.execute_query_select(q, limit=1)
            if len(r) > 0:
                r0 = r[0]
                task = row2task(r0)
            else:
                return None

            if task:
                q_update = f"update task set status = 'in_progress',worker_id = '{worker_id}', lines = 0, ts= now() where id = {task.id} and status = 'open';"
                count = self.execute_query_update(q_update)

                if count == 1:
                    return self.get_task(task.id)
                else:
                    print("Collision detected. Repeat")

    def add_task(self, name) -> Task:
        def call(conn, cur):
            cur.execute(f"insert into task (name, status, ts, fail_count) values ('{name}','open', now(), 0) RETURNING id;")
            task_id = cur.fetchone()[0]
            conn.commit()
            return task_id

        task_id = self.query_template(call)
        return self.get_task(task_id)


if __name__ == '__main__':
    db_util = DbUtil()
    r=db_util.lock_task()
