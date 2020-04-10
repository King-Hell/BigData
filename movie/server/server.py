import cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf


def init_spark_context():
    """初始化spark"""
    conf = SparkConf().setAppName("movie_recommendation-server")
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
    return sc


def run_server(app):
    """运行服务器"""
    app_logged = TransLogger(app)
    # 使用CherryPy作为WSGI服务器
    cherrypy.tree.graft(app_logged, '/')
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    """主函数"""
    # 初始化spark
    sc = init_spark_context()
    # 加载数据库
    dataset_path = os.path.join('datasets', 'ml-latest-small')
    # 创建应用
    app = create_app(sc, dataset_path)
    # 启动服务器
    run_server(app)
