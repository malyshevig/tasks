import logging

from . local_types import Task
from local_util.dbutil import DbUtil

host = "localhost"
port = 5432
user = "postgres"
password = "begemot"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def row2task(row):
    return Task(id=row[0], name=row[1], status=row[2], worker_id=row[3], ts=row[4], lines=row[5],
               fail_count=row[6]) if row else None


class Db(DbUtil):
    instance = None

    @staticmethod
    def get_instance():
        if Db.instance is None:
            Db.instance = Db()

        return Db.instance


    def __init__(self):
        super().__init__("report")

    def update_outdated_tasks(self)-> int:
        c = 0
        q1 = "update task set status='open', fail_count = fail_count+1, ts = now() "
        q1+= "where status = 'in_progress' "
        q1+= "and ts <= (NOW() - INTERVAL '20 SECONDS') "
        q1+= "and fail_count<2;"
        c = c + self.execute_query_update (q1)

        q2 = "update task set status='failed', fail_count = fail_count+1, ts = now() "
        q2+= "where status = 'in_progress' "
        q2+= "and ts <= (NOW() - INTERVAL '20 SECONDS') "
        q2+= "and fail_count>=2;"

        c = c + self.execute_query_update (q2)
        logging.info(f"Updated {c} tasks")
        return c

    def update_task_status(self, task: Task):
        q = f"select * from task where task.id = {task.id};"
        r = self.execute_query_select(q, 1)
        if len(r) == 0:
            return 0

        t:Task = row2task(r[0])

        q_update =  f"update task set status = '{task.status}', lines= {task.lines}, ts = now() where id = {task.id}"
        q_update += f" and status = '{t.status}'"

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
        q = "select id, name, status, worker_id, ts,lines,fail_count from task order by id limit 1000;"
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
                    logging.info("Collision detected. Repeat")

    def lock_task2(self, worker_id) -> Task:
        assert worker_id is not None, f"Unexpected parameter value worker_id: {worker_id}"

        q = f"""update task set status = 'in_progress',worker_id = '{worker_id}', lines = 0, ts= now()
                where status = 'open'
                and id = (select id from task where status = 'open' order by id limit 1 for update skip locked)
                returning id, name, status, worker_id, ts, lines, fail_count;
                """

        r = self.execute_query_update_and_select(q, limit=1)
        return row2task(r[0]) if len(r) > 0 else None

    def add_tasks(self, names:[]) -> Task:
        def call(conn, cur):
            for name in names:
                cur.execute(f"insert into task (name, status, ts, fail_count) values ('{name}','open', now(), 0) RETURNING id;")

            conn.commit()

        self.query_template(call)



