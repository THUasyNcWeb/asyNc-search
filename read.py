"""
    Description: Reading data from DB
"""
import json
import os
import time
import logging
import psycopg2
import schedule
import lucene

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StoredField
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
        query = r'select count(*) from news;'
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
            logger.info(str("Current Id: "+str(result[0])))
            logger.info(str("Current URL: "+str(result[1])))
            data['news_id'] = str(result[0])
            data['news_url'] = result[1]
            data['media'] = result[2]
            data['category'] = result[3]
            data['tags'] = result[4]
            print(result[5])
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
        document.add(StoredField("news_id", data_json['news_id']))
        document.add(StoredField("pub_time", data_json['pub_time']))
        document.add(TextField("title", data_json['title'], Field.Store.YES))
        document.add(TextField("content", data_json['content'], Field.Store.YES))
        document.add(TextField("media", data_json['media'], Field.Store.YES))
        document.add(TextField("category", str(data_json['category']), Field.Store.YES))
        document.add(TextField("tags", str(data_json['category']), Field.Store.YES))
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


if __name__ == "__main__":
    current_id = [0]
    if not os.path.exists('count.txt'):
        with open("count.txt", 'w') as file_writer:
            file_writer.write("0")
    with open('count.txt', 'r') as file_read:
        max_id = file_read.readline()
        current_id[0] = int(max_id)
    mysearch = SearchEngine()

    def read_format(current):
        """_summary_

        Args:
            current (current_id : list): read data from db
        """
        if current[0] > int(mysearch.check_db_status()[0]):
            logger.info("All News Has Been Read!")
        else:
            mysearch.read_from_db(current, 50)
            current[0] = current[0] + 50
            with open('count.txt', 'w') as file_write:
                file_write.write(str(current[0]))
    schedule.every(2).seconds.do(read_format, current_id)
    time.sleep(1)
    while True:
        schedule.run_pending()
