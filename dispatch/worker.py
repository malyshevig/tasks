import getopt
import sys
import time
from random import random, seed
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

from dispatch.local_types import Task
import requests

class RestUnstable:
    def __init__(self, error_probability = 0.1):
        seed(time.time())

        if error_probability > 0:
            self.error_probability = error_probability
        else:
            self.error_probability = 0

    def get(self, url, timeout=30):
        if self.error_probability > 0:
            if random() < self.error_probability:
                time.sleep(2)
                raise Exception("RestUnstable error")
            else:
                return requests.get(url, timeout=timeout)
        else:
            return requests.get(url, timeout=timeout)

    def put(self, url, json):
        if self.error_probability > 0:
            if random() < self.error_probability:
                time.sleep(2)
                raise Exception("RestUnstable error")
            else:
                return requests.put(url, json=json)
        else:
            return requests.put(url, json=json)

    def post(self, url, json):
        if self.error_probability > 0:
            if random() < self.error_probability:
                time.sleep(2)
                raise Exception("RestUnstable error")
            else:
                return requests.post(url, json=json)
        else:
            return requests.post(url, json=json)


rest = RestUnstable(0.0)


def get_task_to_work(worker_id) -> Task:
    url = f"http://127.0.0.1:8080/api/gettask/{worker_id}"
#    response = requests.get(url, timeout=5)
    response = rest.get(url, timeout=5)
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


def update_task_status(task: Task, tries=3) -> bool:

    url = f"http://127.0.0.1:8080/api/task/{task.id}/status"
    data = task.to_json()

#    response = requests.put(url, json=data)
    for _ in range(tries):
        try:
            response = rest.put(url, json=data)
            if response.status_code == 401:
                return False

            if response.status_code != 200:
                raise Exception(f"Update task status error {response.status_code}")

            return True
        except Exception as e:
            logging.error (e)

    raise  Exception(f"Update task status error retries number({tries}) exceeded")



def process_task(task):
    logging.info(f"Processing task {task}")
    try:
        for l in range(101):
            task.lines = l
            if l % 10 == 0:
                if not update_task_status(task):
                    logging.error(f"Stop Processing task {task}")
                    return

            time.sleep(0.5)
            # doing something

        task.status = "done"
        update_task_status(task)
        logging.info(f"Processing task {task} finished")
    except Exception as e:
        logging.error (f"worker:{worker_id}. Task processing is aborted task={task} ")

def do_process_cycle(worker_id: str):
    while True:
        try:
            task = get_task_to_work(worker_id)
            logging.info(f"Received task {task}")
            if task is None:
                time.sleep(1)
            else:
                process_task(task)
        except Exception as e:
            logging.error (e)

if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", ["id="])
    except getopt.GetoptError:
        logging.error("Использование: python script.py -i <id>/n")
        sys.exit(2)

    worker_id = None
    for opt, arg in opts:
        if opt in ("-i", "--id"):
            logging.info(f"Id: {arg}")
            worker_id = arg
    logging.info(f"worker_id={worker_id}")
    do_process_cycle(worker_id)
