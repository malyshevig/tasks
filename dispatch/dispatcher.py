import getopt
import json
import sys

from flask import Flask, jsonify, request
from dispatch.local_types import Task
from dispatch.db import DbUtil

app = Flask(__name__)
db = DbUtil ()

#
# Add a new task with status 'open', for demonstration only
#
@app.route('/api/task', methods=['POST'])
def add_task():
    task_name = json.loads(request.get_json())['name']
    task = db.add_task(task_name)
    return jsonify(task), 200

#
#
#   Get all tasks, for ui demonstration only
#
@app.route('/api/tasks', methods=['GET'])
def get_tasks ():
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
    task = db.get_working_task_for_worker(worker_id)
    if task is not None:
        return jsonify(task), 200

    task = db.lock_task(worker_id)
    print (f"task = {task}")
    if task is None:
        return jsonify({
            "message": "No tasks available"
        }), 204

    return jsonify(task), 201


@app.route('/api/task/<int:task_id>/status', methods=['PUT'])
def update_task_status(task_id):
    if request.method == 'PUT':
        task_json = request.get_json()

        task_dict = json.loads(task_json)
        task = Task(**task_dict)

        print (f"task = {task}")

        local_task = db.get_task(task_id)
        if local_task.worker_id == task.worker_id and local_task.status == "in_progress":
            db.update_task_status(task)
            return jsonify({
                "message": "Task updated successfully"}), 200
        else:
            print (f"local task worker does not match {local_task.worker_id } != {task.worker_id}")
            return jsonify({
                "message": f"This worker is not owner of the task id{task_id}"}  ), 401


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:", ["port="])
    except getopt.GetoptError:
        print("Использование: python dispatcher.py -p <port>")
        sys.exit(2)

    port = None
    for opt, arg in opts:
        if opt in ("-p", "--port"):
            port = arg
    print(f"port={port}")
    if port is None:
        port = 5000

    app.run(debug=True, port=port)


