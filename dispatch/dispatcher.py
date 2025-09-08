import getopt
import json
import sys
import logging
import threading as th
import re

from flask import Flask, jsonify, request, Response
from dispatch.local_types import Task
from dispatch.db import Db
from dispatch.db import monitor_pool_metrics

from prometheus_client import (
    Counter, Gauge, Histogram, generate_latest,
    CONTENT_TYPE_LATEST, CollectorRegistry, multiprocess
)
import time
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)


def normalize_path(path):
    # Преобразуем пути типа /update/status/1 и /update/status/2 в /update/status/{id}
    patterns = [
        (r'/api/gettask/\d+', '/api/gettask/worker_id'),
        (r'/api/task/\d+/status', '/api/{task_id}/status'),
        (r'/api/task/\d+/pause', '/api/{task_id}/pause'),
        (r'/api/task/\d+/resume', '/api/{task_id}/resume'),
    ]

    for pattern, replacement in patterns:
        if re.match(pattern, path):
            return replacement

    return path  # Возвращаем исходный путь, если он не соответствует шаблонам


@app.before_request
def before_request():
    request.start_time = time.time()


@app.after_request
def after_request(response):
    # Пропускаем метрики, чтобы не засорять статистику
    if request.path == '/metrics':
        return response

    # Измеряем время выполнения
    latency = time.time() - request.start_time
    normalized_path = normalize_path(request.path)
    REQUEST_LATENCY.labels(normalized_path).observe(latency)

    # Увеличиваем счетчик запросов
    REQUEST_COUNT.labels(
        request.method,
        request.path,
        response.status_code
    ).inc()

    logging.info(f"after_request: {request.path} {response.status_code} {latency} seconds")
    return response

@app.route('/metrics')
def metrics():
    """Endpoint для предоставления метрик Prometheus"""
    # Для многопроцессного режима (если используется Gunicorn с несколькими воркерами)
    if 'prometheus_multiproc_dir' in os.environ:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)
    else:
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

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

    db.add_tasks(names)

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


@app.route('/api/task/<int:task_id>/pause', methods=['POST'])
def pause_task(task_id):

    if request.method == 'POST':
        db = Db.get_instance()
        task = db.update_task_pause_resume(task_id, 'paused')
        if task is None:
            return jsonify({
                "message": f"Task with id {task_id} not found"}), 404
        else:
            if task is not None:
                return jsonify(task), 200


@app.route('/api/task/<int:task_id>/resume', methods=['POST'])
def resume_task(task_id):
    if request.method == 'POST':
        db = Db.get_instance()
        task = db.update_task_pause_resume(task_id, 'open')
        if task is None:
            return jsonify({
                "message": f"Task with id {task_id} not found"}), 404
        else:
            if task is not None:
                return jsonify(task), 200


@app.route('/api/task/<int:task_id>/priority/<int:priority>', methods=['POST'])
def update_task_priority(task_id,priority):
    if request.method == 'POST':
        db = Db.get_instance()
        task = db.update_task_priority(task_id, priority)
        if task is None:
            return jsonify({
                "message": f"Task with id {task_id} not found"}), 404
        else:
            if task is not None:
                return jsonify(task), 200

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

    task = db.lock_task2(worker_id)
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

    REQUEST_COUNT = Counter(
        'http_requests_total',
        'Total HTTP Requests',
        ['method', 'endpoint', 'http_status']
    )
    REQUEST_LATENCY = Histogram(
        'http_request_duration_seconds',
        'HTTP request latency in seconds',
        ['endpoint'],
         buckets=[0.1, 0.5, 1.0, 2.0, 5.0]
    )

    DB_QUERY_DURATION = Histogram(
        'db_query_duration_seconds',
        'Database query duration in seconds',
        ['query_type']
    )

    DB_POOL_SIZE = Gauge(
        'db_pool_size',
        'Number of connections in the database pool',
        ['pool']
    )

    monitor_pool_metrics(DB_POOL_SIZE)

    app.run(debug=True, port=port, use_reloader=False)


