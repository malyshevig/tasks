import os
import random
import time
import sys
import subprocess

from local_util.dbutil import DbUtil
from exec.local_types import Proc
from random import random


class Db (DbUtil):
    def __init__(self):
        super().__init__("pid")

    def add_proc(self, proc: Proc):
        q = "INSERT INTO proc ( pid, ppid, tag, cmd) " + f"values ({proc.pid}, {proc.ppid}, '{proc.tag}', '{proc.cmd}')"
        self.execute_query_update(q)

    def get_proc_list(self):
        rows = self.execute_query_select(query="select pid, ppid, tag, cmd from proc", limit=0)
        return [Proc(p[0], p[1], p[2], p[3]) for p in rows]

    def del_proc(self, pid):
        q = f"delete from proc where pid = {pid}"
        self.execute_query_update(q)
        pass


def main ():
    db = Db()

    tag = sys.argv[1]
    args = sys.argv[2:]
    ppid = os.getpid()

    while True:
        try:
            print("Запуск основного скрипта...")
            # Запуск основного скрипта (замените 'main_script.py' на имя вашего скрипта)
            proc = subprocess.Popen (args)
            s = ""
            for a in args:
                s += " " + a

            db.add_proc(Proc(proc.pid, ppid, tag, s))
            proc.wait()

            db.del_proc(proc.pid)

        except Exception as e:
            print(f"Ошибка: {e}. Повторный запуск через 30 секунд...")
            time.sleep(30)

if __name__ == '__main__':
    main()