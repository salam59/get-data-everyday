from datetime import datetime
from decouple import config
from tzlocal import get_localzone
from celery import Celery
from celery.schedules import crontab
# from thirteenf.tasks import run
# #getting timezone
#  # Get the local timezone
# local_timezone = get_localzone()

# # Get the current time in the local timezone
# current_time_in_local = datetime.now(local_timezone)

# # Get the timezone offset as "+05:30" format
# current_timezone_offset = current_time_in_local.strftime('%z')



app = Celery(
    'project',
    broker=config('REDIS'),  # Update with your broker URL
    backend="rpc://", # Update with your result backend URL
    include=['tasks']
)
timezone = 'Asia/Kolkata'
app.conf.timezone = timezone
app.conf.beat_schedule = {
        'database_fill_task': {
            'task': 'tasks.run',
            'args': ('2023-08-30','2023-08-31'),
            'schedule': crontab(minute='40'),  # Run every 5 minutes
        },
    }
    
    