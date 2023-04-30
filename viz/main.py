import sqlite3
from flask import Flask
from flask import send_file
import pandas as pd
pd.options.plotting.backend = "plotly"


app = Flask(__name__)


@app.route('/')
def plot():

    df = get_data()
    return create_plot(df)


def get_data():
    conn = sqlite3.connect("warehouse/logs.db")

    df = pd.read_sql_query("SELECT * FROM logs", conn)

    conn.close()

    return df


def create_plot(df):
    """
    Visualizes the data and serves the image
    """
    g_df = df.groupby(["level", "module"])[
        "module"].count().reset_index(name="counts")

    print(g_df)

    pl = g_df.plot.bar(x="level", y="counts")
    pl.write_image("viz/export_data/plot.png")

    return send_file("export_data/plot.png", as_attachment=True)


if __name__ == "__main__":
    app.run(port=8001)
