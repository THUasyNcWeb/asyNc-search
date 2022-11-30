"""
    Description: Reading data from DB
"""
import json
import os
import time
import logging
import threading
import psycopg2
import schedule
import lucene

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StoredField, StringField
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.index import IndexWriter, Term
from org.apache.lucene.index import IndexWriterConfig
from org.apache.lucene.store import FSDirectory

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SearchEngine():
    """
    Class SearchEngin provide basic search function for searching
    """

    def __init__(self):
        """
        init lucene and database
        """
        lucene.initVM()
        self.analyzer = SmartChineseAnalyzer()
        self.indexconfig = IndexWriterConfig(self.analyzer)
        '''
        Connect to the database
        '''
        with open('config/config.json', 'r', encoding='utf-8') as file:
            config = json.load(file)
        self.postgres = (config['hostname'], config['port'],
                         config['username'], config['password'],
                         config['database'])
        self.connection = psycopg2.connect(
            host=self.postgres[0], port=self.postgres[1],
            user=self.postgres[2], password=self.postgres[3],
            dbname=self.postgres[4])
        self.cur = self.connection.cursor()
        self.count = 0
        self.init = False
        if os.path.exists('count'):
            with open('count', 'r', encoding='utf-8') as f_file:
                try:
                    self.count = int(f_file.readlines()[0])
                except Exception as error:
                    print(error)
                    self.count = 0

    def check_db_status(self):
        """
        Function to check the status of bd
        """
        query = r'select max(id) from news;'
        self.cur.execute(query)
        result = self.cur.fetchall()
        return result[0]

    def read_from_db(self, start, num):
        """
        Function to read data from status
        """
        logger.info("Start Reading Data From db")
        query = ('select * from news where id>={i} and id<{j};'.format(i=start[0], j=start[0]+num))
        print(query)
        self.cur.execute(query)
        results = self.cur.fetchall()
        for result in results:
            data = {}
            logger.info(str("Current Id: "+str(result[0])+"\nCurrent URL: "+str(result[1])))
            data['news_id'] = str(result[0])
            data['news_url'] = result[1]
            data['media'] = result[2]
            data['category'] = result[3]
            data['tags'] = result[4]
            data['title'] = result[5]
            data['content'] = result[7]
            data['first_img_url'] = result[8]
            if data['first_img_url'] is None:
                data['first_img_url'] = "None"
            data['pub_time'] = str(result[9])
            data['title'] = result[5]
            data['content'] = result[7]
            self.add_news(data)
        return len(results)

    def get_document(self, data_json):
        """
        Return standard article type
        """
        self.init = True
        document = Document()
        # 给文档对象添加域
        # add方法: 把域添加到文档对象中, field参数: 要添加的域
        # TextField: 文本域, 属性name:域的名称, value:域的值, store:指定是否将域值保存到文档中
        # StringField: 不分词, 作为一个整体进行索引
        if data_json['first_img_url'] is None or data_json['first_img_url'] == '':
            data_json['first_img_url'] = "None"
        document.add(StoredField("first_img_url", data_json['first_img_url']))
        document.add(StoredField("news_url", data_json['news_url']))
        document.add(StringField("news_id", str(data_json['news_id']), Field.Store.YES))
        document.add(StoredField("pub_time", data_json['pub_time']))
        document.add(TextField("title", data_json['title'], Field.Store.YES))
        document.add(TextField("content", data_json['content'], Field.Store.YES))
        document.add(TextField("media", data_json['media'], Field.Store.YES))
        document.add(TextField("category", str(data_json['category']), Field.Store.YES))
        document.add(TextField("tags", str(data_json['tags']), Field.Store.YES))
        return document

    def add_news(self, data_json, file_path="index"):
        """add_news

        Args:
            data_json (_type_): data from crawler
            file_path (str, optional): index file

        Returns:
            _type_: Bool:Success False:Failed

        """
        if os.path.exists(file_path) is False:
            os.mkdir(file_path)
        try:
            data_json = json.dumps(data_json)
            data_json = json.loads(data_json, strict=False)
        except ValueError:
            return False
        try:
            analyzer = self.analyzer
            indexconfig = IndexWriterConfig(analyzer)
            directory = FSDirectory.open(Paths.get(file_path))
            indexwriter = IndexWriter(directory, indexconfig)
            document = self.get_document(data_json)
            term = Term("news_id", data_json['news_id'])
            indexwriter.updateDocument(term, document)
            indexwriter.close()
            self.count += 1
            try:
                with open('count', 'w', encoding='utf-8') as f_file:
                    f_file.write(str(self.count))
            except Exception as error:
                print(error)
            return True
        except Exception as error:
            print(error)
            return False

    def read_thread(self, start, index_path, total=5000):

        """open read threads

        Args:
            start (_type_): where to start
            file_path (str, optional): _description_. Defaults to "index".
        """
        indexwriters = []
        for file_path in index_path:
            indexconfig = IndexWriterConfig(self.analyzer)
            directory = FSDirectory.open(Paths.get(file_path))
            indexwriters += [IndexWriter(directory, indexconfig)]

        def read_db(indexwriter, start=start, num=500):
            """_summary_

            Args:
                indexwriter (_type_): _description_
                start (_type_, optional): _description_. Defaults to start.
                num (int, optional): _description_. Defaults to 500.
            """
            logger.info("Start Reading Data From db")
            query = ('select * from news where id>={i} and id<{j};'.format(i=start, j=start+num))
            print(query)
            self.cur.execute(query)
            results = self.cur.fetchall()
            data = {}
            for result in results:
                logger.info(str("Current Id: "+str(result[0])+"\nCurrent URL: "+str(result[1])))
                data['news_id'] = str(result[0])
                data['news_url'] = result[1]
                data['media'] = result[2]
                data['category'] = result[3]
                data['tags'] = result[4]
                data['title'] = result[5]
                data['content'] = result[7]
                data['first_img_url'] = result[8]
                if data['first_img_url'] is None:
                    data['first_img_url'] = "None"
                data['pub_time'] = str(result[9])
                data['title'] = result[5]
                data['content'] = result[7]
                document = self.get_document(data)
                term = Term("news_id", data['news_id'])
                indexwriter.updateDocument(term, document)
            indexwriter.close()
        average = int(total / 10)
        for index in range(0, 10):
            thread = threading.Thread(target=read_db(indexwriters[index],
                                                     current_id[0]+average*index, num=average))
            thread.start()
        start[0] = start[0] + average * 10
        with open('dbcount/count.txt', 'w', encoding="utf-8") as file_write:
            file_write.write(str(start[0]))


if __name__ == "__main__":

    mysearch = SearchEngine()
    current_id = [0]
    if not os.path.exists('dbcount'):
        os.mkdir("dbcount")
        with open("dbcount/count.txt", 'w', encoding="utf-8") as file_writer:
            file_writer.write("0")
    if not os.path.exists('dbcount/count.txt'):
        with open("dbcount/count.txt", 'w', encoding="utf-8") as file_writer:
            file_writer.write("0")
    with open('dbcount/count.txt', 'r', encoding="utf-8") as file_read:
        max_id = file_read.readline()
        current_id[0] = int(max_id)
    dir_list = []
    if not os.path.exists('index'):
        os.mkdir('index')
    for i in range(1, 11):
        dir_list += ['index/index' + str(i)]
        if not os.path.exists('index/index'+str(i)):
            os.mkdir('index/index' + str(i))

    def read_format_threading(current):
        """_summary_

        Args:
            current (current_id : list): read data from db
        """
        db_total = int(mysearch.check_db_status()[0])
        total = min(5000, db_total-current[0])
        total_info = str("total: "+str(db_total))
        write = str(total)+" to be write"
        logger.info(total_info)
        logger.info(write)
        mysearch.read_thread(current, dir_list, total)

    schedule.every(1).seconds.do(read_format_threading, current_id)
    time.sleep(1)
    while True:
        schedule.run_pending()
