# main.py
from celeryconfig import app,timezone
from tasks import run
from celery.schedules import crontab


if __name__ == '__main__':
    app.conf.beat_schedule = {
        'database_fill_task': {
            'task': 'run',
            'args': ('2023-08-30','2023-08-31'),
            'schedule': crontab(minute='*/1'),  # Run every 5 minutes
        },
    }
    
    app.conf.update({
        'timezone': timezone,
        'enable_utc': True,
    })

    app.worker_main(['worker', '--loglevel=INFO'])


# celery -A arigato worker --loglevel=info
