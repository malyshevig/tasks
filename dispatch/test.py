import json

import requests
from random import randint

from dispatch.local_types import Task


def add_task(n:int):
    url = "http://127.0.0.1:5004/api/task"
    task_list = [{"name": f"file_{randint(0,255)}.txt"} for _ in range(n)]
    json_s = json.dumps(task_list)

    response = requests.post(url, json=json_s)
    response.raise_for_status()  # Проверка на ошибки (4xx/5xx)

    print(f"Added response: {response}")

if __name__ == "__main__":
     for i in range(100):
        add_task(100000)
        print (f"added {i}")
