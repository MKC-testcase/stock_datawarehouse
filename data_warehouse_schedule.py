from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from data_warehouse_update import dw_update

def start_updates():
    scheduler = BlockingScheduler()
    scheduler.add_job(dw_update, 'cron', day= "mon-fri", hour='16')
    scheduler.start()

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(dw_update, 'cron', day= "mon-fri", hour='16')
    scheduler.start()