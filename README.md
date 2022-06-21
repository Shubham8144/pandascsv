# pandascsv
celery : celery -A application.celery worker --pool=solo -Q celery,queue1 --loglevel=info
