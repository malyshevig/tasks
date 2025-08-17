import getopt
import json
import sys
import logging
import threading as th

from flask import Flask, jsonify, request
from dispatch.local_types import Task
from dispatch.db import Db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)


#
# Add a new task with status 'open', for demonstration only
#
@app.route('/api/task', methods=['POST'])
def add_task():
    logging.info(f"add_task {th.current_thread()}")
    db = Db.get_instance()
   # task_name = json.loads(request.get_json())['name']
    json_data = json.loads(request.get_json())
    names = [x['name'] for x in json_data]
    print (f"adding {names}")

    db.add_tasks(names)
    print(f"added")
    return jsonify("result ok"), 200

#
#
#   Get all tasks, for ui demonstration only
#
@app.route('/api/tasks', methods=['GET'])
def get_tasks ():
    logging.info(f"get_tasks_list {th.current_thread()}")
    db = Db.get_instance()

    tasks = db.get_tasks()
    return jsonify(tasks), 200

#
#   a worker requests a task for processing
#   if case the worker is already assigned to the task return that task
#   worker assigned to a task means there is a task in status 'in_progress' and worker_id = <this worker>
#   if worker is free, find an open task set status to "in_progress" and assign worker_id
#   when task is being assigned to a worker a special logic is used to avoid assiging the task to two or more workers
#   refer to DvUtil.lock_task() method
#

@app.route('/api/gettask/<int:worker_id>', methods=['GET'])
def locktask(worker_id):
    logging.info(f"get_task {th.current_thread()}")
    db = Db.get_instance()

    task = db.get_working_task_for_worker(worker_id)
    if task is not None:
        return jsonify(task), 200

    task = db.lock_task(worker_id)
    logging.info (f"locktask: task = {task}")
    if task is None:
        logging.info(f"locktask: return 204 - no task is avalilable")
        return jsonify({
            "message": "No tasks available"
        }), 204
    logging.info(f"locktask: return 201 - ok")
    return jsonify(task), 201


@app.route('/api/task/<int:task_id>/status', methods=['PUT'])
def update_task_status(task_id):
    logging.info(f"update_task_status {th.current_thread()}")
    if request.method == 'PUT':
        task_json = request.get_json()

        task_dict = json.loads(task_json)
        task = Task(**task_dict)

        logging.info(f"update_task_status task = {task}")
        db = Db.get_instance()

        local_task = db.get_task(task_id)
        if local_task.worker_id == task.worker_id and local_task.status == "in_progress":
            db.update_task_status(task)
            logging.info(f"update_task_status return 200")
            return jsonify({
                "message": "Task updated successfully"}), 200
        else:
            logging.info (f"local task worker does not match {local_task.worker_id } != {task.worker_id}")
            logging.info(f"update_task_status return 401")
            return jsonify({
                "message": f"This worker is not owner of the task id{task_id}"}  ), 401


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:", ["port="])
    except getopt.GetoptError:
        logging.info("Использование: python dispatcher.py -p <port>")
        sys.exit(2)

    port = None
    for opt, arg in opts:
        if opt in ("-p", "--port"):
            port = arg

    if port is None:
        port = 5000

    app.run(debug=True, port=port)


