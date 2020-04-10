class Config:
    USE_ALS_THRESHOLD = 5  # 使用ALS为用户推荐电影的最小评分数
    MODEL_UPDATE_NUM = 5  # ALS重新训练的频率
    MOVIE_RATING_COUNT_THRESHOLD = 25  # 电影进入推荐系统的最小评分数
