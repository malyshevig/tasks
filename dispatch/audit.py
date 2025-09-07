import logging
import time
from random import gauss

from dispatch.db import Db
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def app_cycle():
    db = Db.get_instance()
    time.sleep(120)    # initial warming of dispatchers and worker
    while True:
        c = db.update_outdated_tasks()
        if c > 0:
            logging.info(f"audit: {c} tasks updated")
        time.sleep(abs(gauss(mu=2, sigma=0.5)))


if __name__ == '__main__':
    app_cycle()