import sqlite3
from flask import Flask
import pandas as pd


app = Flask(__name__)


@app.route('/')
def plot():

    df = get_data()
    create_plot(df)

    return "hi"


def get_data():
    conn = sqlite3.connect("warehouse/logs.db")

    df = pd.read_sql_query("SELECT * FROM logs", conn)

    conn.close()

    return df


def create_plot(df):
    g_df = df.groupby(["level", "module"])["module"].count()
    g_df.to_csv("viz/export_data/out.csv")


if __name__ == "__main__":
    app.run(port=8001)
