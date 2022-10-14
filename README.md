# pandascsv
import data from csv file in background with flask server.
celery : celery -A application.celery worker --pool=solo -Q celery,queue1 --loglevel=info
