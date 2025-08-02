import getopt
import sys
import time

from dispatch.local_types import Task
import requests

def get_task_to_work(worker_id) -> Task:
    url = f"http://127.0.0.1:8080/api/gettask/{worker_id}"
    response = requests.get(url, timeout=5)
    response.raise_for_status()  # Проверка на ошибки (4xx/5xx)
    if response.status_code == 204:
        return None
    elif response.status_code in (200, 201):
        data = response.json()
        task = Task(id=data['id'], name=data['name'], status=data['status'], ts=data['ts'], lines=data['lines'],
                    worker_id=data['worker_id'])
        return task
    else:
        raise Exception(f"Unknown response code {response.status_code}")


def update_task_status(task: Task) -> bool:
    url = f"http://127.0.0.1:8080/api/task/{task.id}/status"
    data = task.to_json()

    response = requests.put(url, json=data)
    if response.status_code == 401:
        return False

    if response.status_code != 200:
        raise Exception(f"Update task status error {response.status_code}")

    return True


def process_task(task):
    print(f"Processing task {task}")
    try:
        for l in range(1000):
            task.lines = l
            if l % 10 == 0:
                if not update_task_status(task):
                    print(f"Stop Processing task {task}")
                    return

            time.sleep(0.1)
            # doing something

        task.status = "done"
        update_task_status(task)
        print(f"Processing task {task} finished")
    except Exception as e:
        task.status = "failed"
        update_task_status(task)


def do_process_cycle(worker_id: str):
    while True:
        task = get_task_to_work(worker_id)
        print(f"Received task {task}")
        if task is None:
            time.sleep(1)
        else:
            process_task(task)


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", ["id="])
    except getopt.GetoptError:
        print("Использование: python script.py -i <id>/n")
        sys.exit(2)

    worker_id = None
    for opt, arg in opts:
        if opt in ("-i", "--id"):
            print(f"Id: {arg}")
            worker_id = arg
    print(f"worker_id={worker_id}")
    do_process_cycle(worker_id)
