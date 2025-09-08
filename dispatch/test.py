import json

import requests
from random import randint

from dispatch.local_types import Task


def add_task(n:int):
    url = "http://127.0.0.1:8080/api/task"
    task_list = [{"name": f"file_{randint(0,255)}.txt"} for _ in range(n)]
    json_s = json.dumps(task_list)

    response = requests.post(url, json=json_s)
    response.raise_for_status()  # Проверка на ошибки (4xx/5xx)

    print(f"Added response: {response}")

if __name__ == "__main__":
    pack = 10
    added = 0
    for i in range(100):
        add_task(pack)
        added += pack
        print (f"added {added}")
