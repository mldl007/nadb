import pymongo
import os


class DBOperations:
    """
    Inserts processed news into MongoDB
    """
    def __init__(self):
        self.url = os.getenv('DB_URL')
        self.database = "rss_news_db"
        self.collection = "rss_news"
        self.__client = None
        self.__error = 0

    def __connect(self):
        try:
            self.__client = pymongo.MongoClient(self.url)
            _ = self.__client.list_database_names()
        except Exception as conn_exception:
            self.__error = 1
            self.__client = None
            raise

    def __insert(self, documents):
        try:

            db = self.__client[self.database]
            coll = db[self.collection]
            coll.drop()
            coll.insert_many(documents=documents)
        except Exception as insert_err:
            self.__error = 1
            raise

    def __close_connection(self):
        if self.__client is not None:
            self.__client.close()
            self.__client = None

    def insert_news_into_db(self, documents: list):
        if self.url is not None:
            if self.__error == 0:
                self.__connect()
            if self.__error == 0:
                self.__insert(documents=documents)
            if self.__error == 0:
                print("Insertion Successful")
            if self.__client is not None:
                self.__close_connection()
