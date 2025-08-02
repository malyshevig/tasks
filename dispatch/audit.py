import time
from random import gauss

from dispatch.db import DbUtil


def app_cycle():
    db = DbUtil()
    while True:
        c = db.update_outdated_tasks()
        print(f"audit: {c} tasks updated")
        time.sleep(abs(gauss(mu=1, sigma=0.5)))


if __name__ == '__main__':
    app_cycle()