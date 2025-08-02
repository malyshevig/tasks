import json

import requests

from dispatch.local_types import Task


def add_task():
    url = "http://127.0.0.1:5000/api/task"
    response = requests.post(url, json=json.dumps({"name": "1.txt"}), timeout=5)
    response.raise_for_status()  # Проверка на ошибки (4xx/5xx)

    data = response.json()
    task = Task(id=data['id'], name=data['name'], status=data['status'], ts=data['ts'], lines=data['lines'],
                worker_id=data['worker_id'])
    print(f"Added response: {response} task: {task}")


if __name__ == "__main__":
    for x in range(1000):
        add_task()
