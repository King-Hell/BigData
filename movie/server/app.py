from flask import Blueprint
import json
from config import Config
from engine import RecommendationEngine
import logging
from flask import Flask, request
import mysql.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

main = Blueprint('main', __name__)
conn = mysql.connector.connect(host='kinghell.cn', user='movie', password='1234', database='movie')

def query(sql, *args):
    if not conn.is_connected():
        conn.reconnect()
    cursor = conn.cursor()
    cursor.execute(sql.format(*args))
    return cursor


@main.route("/ratings/top/<int:user_id>/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    """获取用户user_id的count个最佳推荐,GET方式"""
    logger.debug("请求用户%s的%s个最高评分电影", user_id, count)
    sql='SELECT count(*) FROM ratings where userId={};'
    cursor=query(sql,user_id)
    value=cursor.fetchone()[0]
    cursor.close()
    if value>Config.USE_ALS_THRESHOLD:
        top_ratings = recommendation_engine.get_top_ratings(user_id, count)
    else:
        sql='SELECT title,movieId,"无数" FROM movies NATURAL JOIN ratings GROUP BY movieId HAVING count(*)>=25 ORDER BY avg(rating) DESC LIMIT 20;'
        cursor=query(sql)
        top_ratings=cursor.fetchall()
        cursor.close()
    return json.dumps(top_ratings)


@main.route("/ratings/<int:user_id>/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    """获取用户user_id对于电影movie_id的评价,GET方式"""
    logger.debug("请求用户%s对于%s的评分", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)


@main.route("/ratings/add/<int:user_id>", methods=["POST"])
def add_ratings(user_id):
    """新增用户评分,POST方式"""
    ratings_list = list(request.form.keys())[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # 调用引擎方法增加评分数据
    recommendation_engine.add_ratings(ratings)
    return json.dumps(list(ratings))


@main.route("/ratings/add/<int:user_id>/<int:movie_id>/<string:rating>", methods=["GET"])
def add_rating(user_id, movie_id, rating):
    rating = float(rating)
    result = (user_id, movie_id, rating)
    recommendation_engine.add_rating(result)
    sql = 'INSERT INTO ratings VALUES({},{},{},NOW()) ON DUPLICATE KEY UPDATE rating={},timestamp=NOW()'
    cursor=query(sql, user_id, movie_id, rating, rating)
    conn.commit()
    cursor.close()
    return "OK"


@main.route("/movies/genre/<string:genre>", methods=["GET"])
def get_genre(genre):
    sql = 'SELECT * FROM movies WHERE genres LIKE "%{}%";'
    cursor = query(sql, genre)
    values=cursor.fetchall()
    cursor.close()
    return json.dumps(values)


@main.route("/movies/getimdb/<int:id>", methods=["GET"])
def get_imdb(id):
    logger.debug('获取imdbID')
    sql = 'SELECT imdbId,avg(rating)as rating  FROM links NATURAL JOIN ratings WHERE movieId={};'
    cursor = query(sql, id)
    value=cursor.fetchone()
    cursor.close()
    return json.dumps(value)

@main.route("/movies/similar/<int:id>/<int:count>",methods=["GET"])
def get_similar(id,count):
    result=recommendation_engine.get_similar_movies(id,count)
    return json.dumps(result)

@main.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def create_app(spark_context, dataset_path):
    """创建Web应用"""
    global recommendation_engine
    # 初始化推荐引擎
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)
    # 创建Flask应用
    app = Flask(__name__)
    # 绑定蓝图
    app.register_blueprint(main)
    return app
