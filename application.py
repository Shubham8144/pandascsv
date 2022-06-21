from flask import Flask, render_template, request, redirect, url_for
import os
from celery import Celery
import pandas as pd
import mysql.connector

app = Flask(__name__)

app.config["DEBUG"] = True
app.config["CELERY_RESULT_BACKEND"] = "db+mysql+pymysql://root:root@localhost/celery"
app.config[
    "CELERY_BROKER_URL"
] = "redis://:yjBZwjDJ4c6xKkxQ3QTj6qZ+VG1LPCuEtDXQYWfEdN8Ma8QCYfloN92e8wRlEfxG94LCH65lB54WUZc@localhost:6379/0"
UPLOAD_FOLDER = "static/files"
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

mydb = mysql.connector.connect(
    host="127.0.0.1", user="root", password="root", database="csvdata"
)

mycursor = mydb.cursor()
mycursor.execute("SET GLOBAL max_allowed_packet=67108864")


@app.route("/")
def index():
    return render_template("index.html")


celery = Celery(__name__)
celery.conf.broker_url = app.config["CELERY_BROKER_URL"]
celery.conf.result_backend = app.config["CELERY_RESULT_BACKEND"]


@celery.task(name="create_task", queue="queue1")
def parseCSV(filePath):
    mycursor.execute(
        "CREATE TABLE IF NOT EXISTS records (id INT AUTO_INCREMENT PRIMARY KEY , Region VARCHAR(255), Country VARCHAR(255), Item VARCHAR(255), Type VARCHAR(255), Sales VARCHAR(255), Channel VARCHAR(255))"
    )

    col_names = ["Region", "Country", "Item", "Type", "Sales", "Channel"]
    csvData = pd.read_csv(filePath, names=col_names, header=None)
    for i, row in csvData.iterrows():
        sql = "INSERT INTO records (Region, Country, Item, Type, Sales, Channel) VALUES (%s, %s, %s, %s, %s, %s)"
        value = (
            row["Region"],
            row["Country"],
            row["Item"],
            row["Type"],
            row["Sales"],
            str(row["Channel"]),
        )
        mycursor.execute(sql, value)
        mydb.commit()
        print(
            i,
            row["Region"],
            row["Country"],
            row["Item"],
            row["Type"],
            row["Sales"],
            row["Channel"],
        )


@app.route("/", methods=["POST"])
def uploadFiles():
    uploaded_file = request.files["file"]
    if uploaded_file.filename != "":
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], uploaded_file.filename)
        uploaded_file.save(file_path)
        parseCSV.apply_async([file_path])
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(port=5000)

