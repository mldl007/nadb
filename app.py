from news_extractor import get_news
from db_operations import DBOperations
import json
from flask import Flask
from flask_cors import cross_origin, CORS

app = Flask(__name__)
CORS(app)


@app.route("/")
@cross_origin()
def update_news():
    status = "success"
    try:
        news_df = get_news()
        news_json = [*json.loads(news_df.reset_index(drop=True).to_json(orient="index")).values()]
        db = DBOperations()
        db.insert_news_into_db(news_json)
    except:
        status = "failure"
    return status


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)