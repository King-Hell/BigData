import os
from pyspark.mllib.recommendation import ALS
import numpy as np
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """平均评分和评分数量计算函数,参数(movieId,ratings_iterable),返回(movieId,(count,avg_rating))"""
    count = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (count, float(sum(x for x in ID_and_ratings_tuple[1])) / count)


class RecommendationEngine:
    """推荐引擎类"""
    """RDD列表：
    ratings_RDD:userId,movieId,rating
    movies_RDD:movieId,title,genres
    movies_titles_RDD:movieId,title
    movies_rating_counts_RDD:movieId,ratings_count
    """

    def __count_and_average_ratings(self):
        """计算电影的平均评分和评分数量"""
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.__ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.__movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        """使用ALS算法训练模型"""
        logger.info("训练ALS模型......")
        self.__model = ALS.train(self.__ratings_RDD, self.__rank, seed=self.__seed,
                                 iterations=self.__iterations, lambda_=self.__regularization_parameter)
        logger.info("ALS模型训练完毕")

    def __predict_ratings(self, user_and_movie_RDD):
        """预测评分，参数(userID, movieID)，返回值(movie_title,movie_id, rating_count)"""
        predicted_RDD = self.__model.predictAll(user_and_movie_RDD)
        # predicted_rating_RDD:(movieId,rating)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        # predicted_rating_title_and_count:(movieId,((rating,movie_title),rating_count))
        predicted_rating_title_and_count_RDD = predicted_rating_RDD.join(self.__movies_titles_RDD).join(
            self.__movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = predicted_rating_title_and_count_RDD.map(
            lambda r: (r[1][0][1], r[0], r[1][1]))
        return predicted_rating_title_and_count_RDD

    def add_ratings(self, ratings):
        """增加新的评分(user_id, movie_id, rating)"""
        new_ratings_RDD = self.__sc.parallelize(ratings)
        self.__ratings_RDD = self.__ratings_RDD.union(new_ratings_RDD)
        # 重新计算平均评价
        self.__count_and_average_ratings()
        # 重新训练模型
        self.__train_model()

    def add_rating(self, rating):
        """增加一个新的评分(user_id, movie_id, rating)"""
        if len(self.__temp_new_ratings) < Config.MODEL_UPDATE_NUM:
            self.__temp_new_ratings.append(rating)
        else:
            self.add_ratings(self.__temp_new_ratings)
            self.__temp_new_ratings.clear()

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """根据用户和电影预测评分,参数(user_id,movie_ids),movie_ids为电影合集"""
        # 将user_id和movie_ids转换为RDD
        requested_movies_RDD = self.__sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # 预测评分
        ratings = self.__predict_ratings(requested_movies_RDD).collect()
        return ratings

    def get_top_ratings(self, user_id, count):
        """返回user_id的movies_count个最高评价电影"""
        # 过滤出所有该用户未评分的电影，然后去重
        user_unrated_movies_RDD = self.__ratings_RDD.filter(lambda rating: not rating[0] == user_id).map(
            lambda x: (user_id, x[1])).distinct()
        # 预测对这些电影的评分,取movies_count个评分数大于等于最低评分数量的电影的电影
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(
            lambda r: r[2] >= Config.MOVIE_RATING_COUNT_THRESHOLD).takeOrdered(count,
                                                                               key=lambda
                                                                                   x: -x[1])
        return ratings

    def get_similar_movies(self, movie_id, count):
        """获取与movie_id相似的count个相似电影"""
        movie_features = self.__model.productFeatures()
        # 取得movie_id的特征向量
        target_feature = movie_features.lookup(movie_id)[0]
        vector = np.array(target_feature)
        # 计算相似度
        similarity = movie_features.map(
            lambda x: (x[0], vector.dot(x[1]) / (np.linalg.norm(vector) * np.linalg.norm(x[1]))))
        # 取count个相似度最高的电影
        result = self.__movies_titles_RDD.join(similarity).top(count, key=lambda x: x[1][1])
        return tuple(map(lambda x:(x[0],x[1][0]),result))

    def __init__(self, sc, dataset_path):
        """初始化推荐引擎"""
        logger.info("启动推荐引擎：")
        self.__sc = sc
        # 加载评分数据
        logger.info("加载评分数据：")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.__sc.textFile(ratings_file_path)
        # 分离表头
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        # 提取userId,movieId,rating,保存成元组
        self.__ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header).map(
            lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()
        # 加载电影数据
        logger.info("加载电影数据")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.__sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        # 将文本转换为元组
        self.__movies_RDD = movies_raw_RDD.filter(lambda line: line != movies_raw_data_header).map(
            lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()
        self.__movies_titles_RDD = self.__movies_RDD.map(lambda x: (int(x[0]), x[1])).cache()
        # 计算初始数据
        self.__count_and_average_ratings()
        # 评价相关参数
        self.__temp_new_ratings = []
        # ALS训练参数
        self.__rank = 8
        self.__seed = 5
        self.__iterations = 10
        self.__regularization_parameter = 0.1
        self.__train_model()
