"""
    Description: Searching Backend for Async Using Pylucene
"""
import json
import os
import math
import re
import threading
from threading import Thread
import psycopg2

import lucene


import gevent
import gevent.pywsgi
import gevent.queue

from tinyrpc.server.gevent import RPCServerGreenlets
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.wsgi import WsgiServerTransport

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, TextField, StoredField
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.index import IndexWriter, Term
from org.apache.lucene.index import IndexWriterConfig, DirectoryReader
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search.highlight import Highlighter, QueryScorer
from org.apache.lucene.search.highlight import SimpleHTMLFormatter, TokenSources
from org.apache.lucene.search import BooleanQuery, TermQuery, BooleanClause


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
        print(result)

    def read_from_db(self):
        """
        Function to read data from status
        """
        print("Start Reading Data From db")
        query = r'select * from news limit 10000 offset 0;'
        self.cur.execute(query)
        results = self.cur.fetchall()
        for result in results:
            data = {}
            print(result[0])
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
            # data['news_id'] = str(result[0])
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
            term = Term("news_url", data_json['news_url'])
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

    def search_keywords(self, keyword, must_contain=[], must_not=[], page=0, file_path='index'):
        """_summary_

        Args:
            keyword (_type_): keyword wanted to be searched
            must_contain (list, optional): keywords must be contained Defaults to [].
            must_not (list, optional): keywords must not be contained Defaults to [].
            page (int, optional): page_num. Defaults to 0.
            file_path (str, optional): index_file Defaults to 'index'.

        Returns:
            _type_: searching result
        """
        # 参数一:默认的搜索域, 参数二:使用的分析器
        # queryParser = QueryParser("content", self.analyzer)
        # 2.2 使用查询解析器对象, 实例化Query对象
        # search_content = 'content:'+str('"'+keyword+'"')
        # query = queryParser.parse(search_content)
        boolquery = BooleanQuery.Builder()
        print(keyword)
        for key in keyword:
            query_content = TermQuery(Term("content", key))
            query_title = TermQuery(Term("title", key))
            boolquery.add(query_content, BooleanClause.Occur.SHOULD)
            boolquery.add(query_title, BooleanClause.Occur.SHOULD)
        for key_must in must_contain:
            query_content = TermQuery(Term("content", key_must))
            query_title = TermQuery(Term("title", key_must))
            boolquery.add(query_content, BooleanClause.Occur.MUST)
            boolquery.add(query_title, BooleanClause.Occur.MUST)
        for key_must_not in must_not:
            query_content = TermQuery(Term("content", key_must_not))
            query_title = TermQuery(Term("title", key_must_not))
            boolquery.add(query_content, BooleanClause.Occur.MUST_NOT)
            boolquery.add(query_title, BooleanClause.Occur.MUST_NOT)
        query = boolquery.build()
        directory = FSDirectory.open(Paths.get(file_path))
        indexreader = DirectoryReader.open(directory)
        searcher = IndexSearcher(indexreader)
        print("Start!")
        try:
            topdocs = searcher.search(query, 100)
            try:
                total = int(str(topdocs.totalHits).replace(" hits", ''))
            except Exception as error:
                print(error)
                total = 990
            total_page = math.ceil(total/10)-1
            if page > total_page + 1 or page < 0:
                news = {}
                news['total'] = 0
                news['news_list'] = []
                news['message'] = "Success"
                return news
            # start = page * 10
            # end = min(start+10,total)
            # topdocs = searcher.search(query, 10)
            # print("total:"+str(topdocs.totalHits))
            # scoredocs = topdocs.scoreDocs[start:end]
            scoredocs = topdocs.scoreDocs[0:100]
            queryscorer = QueryScorer(query)
            simplehtmlformatter = SimpleHTMLFormatter('<span class="szz-type">', '</span>')
            lighter = Highlighter(simplehtmlformatter, queryscorer)
            news_list = []
            for scoredoc in scoredocs:
                docid = scoredoc.doc
                # score = scoredoc.score
                doc = searcher.doc(docid)
                tokenstream = TokenSources.getTokenStream(doc, "content", self.analyzer)
                content = lighter.getBestFragment(tokenstream, doc.get('content'))
                new = {}
                new['title'] = doc.get('title')
                new['media'] = doc.get('media')
                new['url'] = doc.get('news_url')
                new['pub_time'] = doc.get('pub_time')
                new['content'] = content
                new['picture_url'] = doc.get('first_img_url')
                new['tags'] = doc.get('tags')
                if new['picture_url'] == 'None':
                    new['picture_url'] = ""
                news_list += [new]
            indexreader.close()
            news = {}
            news['total'] = int(str(topdocs.totalHits).replace(" hits", ''))
            news['news_list'] = news_list
            news['message'] = "Success"
            return news
        except Exception as error:
            print(error)
            news = {}
            news['total'] = 0
            news['news_list'] = []
            news['message'] = "Error"
            return news

    def search_news(self, keyword, page=0, file_path='index'):
        """_summary_

        Args:
            keyword (_type_): keyword wanted to be searched
            page (int, optional): pagenum. Defaults to 0.
            file_path (str, optional): index file. Defaults to 'index'.

        Returns:
            _type_: news_list
        """
        print("Searching begin")
        try:
            # 参数一:默认的搜索域, 参数二:使用的分析器
            fields = ["content", "title"]
            query_parser = MultiFieldQueryParser(fields, self.analyzer)
            # 2.2 使用查询解析器对象, 实例化Query对象
            query = query_parser.parse([str(keyword), str(keyword)], fields,
                                       [BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD],
                                       self.analyzer)

            directory = FSDirectory.open(Paths.get(file_path))
            indexreader = DirectoryReader.open(directory)
            searcher = IndexSearcher(indexreader)
            topdocs = searcher.search(query, (page+1)*10)
            try:
                total = int(str(topdocs.totalHits).replace(" hits", ''))
            except Exception as error:
                print(error)
                total = 990
            print(total)
            total_page = math.ceil(total/10)-1
            if page > total_page + 1 or page < 0:
                news = {}
                news['total'] = 0
                news['news_list'] = []
                news['message'] = "Success"
                return news
            start = page * 10
            end = min(start+10, total)
            scoredocs = topdocs.scoreDocs
            query_scorer = QueryScorer(query)
            simplehtmlformatter = SimpleHTMLFormatter('<span class="szz-type">', '</span>')
            lighter = Highlighter(simplehtmlformatter, query_scorer)
            news_list = []
            for i in range(end-start):
                scoredoc = scoredocs[start+i]
                docid = scoredoc.doc
                # score = scoreDoc.score
                doc = searcher.doc(docid)
                tokenstream = TokenSources.getTokenStream(doc, "content", self.analyzer)
                content = lighter.getBestFragment(tokenstream, doc.get('content'))
                tokenstream = TokenSources.getTokenStream(doc, "title", self.analyzer)
                title = lighter.getBestFragment(tokenstream, doc.get('title'))
                if title is None:
                    title = doc.get('title')
                if content is None:
                    content = doc.get('content')
                new = {}
                new['title'] = title
                new['media'] = doc.get('media')
                new['url'] = doc.get('news_url')
                new['pub_time'] = doc.get('pub_time')
                new['content'] = content
                new['picture_url'] = doc.get('first_img_url')
                new['tags'] = doc.get('tags')
                if new['picture_url'] == 'None':
                    new['picture_url'] = ""
                news_list += [new]
            indexreader.close()
            print("Searching End!")
            news = {}
            news['total'] = total
            news['news_list'] = news_list
            news['message'] = "Success"
            return news
        except Exception as error:
            print(error)
            news = {}
            news['total'] = 0
            news['news_list'] = []
            news['message'] = "Error"
            return news

    def search_news_thread(self, keyword, file_path='index', page=0):
        """_summary_

        Args:
            keyword (_type_): keyword wanted to be searched
            page (int, optional): pagenum. Defaults to 0.
            file_path (str, optional): index file. Defaults to 'index'.

        Returns:
            _type_: news_list
        """
        print("Searching begin")
        lucene.getVMEnv().attachCurrentThread()
        try:
            # 参数一:默认的搜索域, 参数二:使用的分析器
            fields = ["content", "title"]
            query_parser = MultiFieldQueryParser(fields, self.analyzer)
            # 2.2 使用查询解析器对象, 实例化Query对象
            query = query_parser.parse([str(keyword), str(keyword)], fields,
                                       [BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD],
                                       self.analyzer)

            directory = FSDirectory.open(Paths.get(file_path))
            indexreader = DirectoryReader.open(directory)
            searcher = IndexSearcher(indexreader)
            topdocs = searcher.search(query, 100)
            try:
                total = int(str(topdocs.totalHits).replace(" hits", ''))
            except Exception as error:
                print(error)
                total = 990
            if total > 100:
                total = 100
            total_page = math.ceil(total/10)-1
            if page > total_page + 1 or page < 0:
                news = {}
                news['total'] = 0
                news['news_list'] = []
                news['message'] = "Success"
                return news
            start = page * 10
            end = total
            scoredocs = topdocs.scoreDocs
            query_scorer = QueryScorer(query)
            simplehtmlformatter = SimpleHTMLFormatter('<span class="szz-type">', '</span>')
            lighter = Highlighter(simplehtmlformatter, query_scorer)
            news_list = []
            for i in range(end-start):
                scoredoc = scoredocs[start+i]
                docid = scoredoc.doc
                score = scoredoc.score
                doc = searcher.doc(docid)
                tokenstream = TokenSources.getTokenStream(doc, "content", self.analyzer)
                content = lighter.getBestFragment(tokenstream, doc.get('content'))
                tokenstream = TokenSources.getTokenStream(doc, "title", self.analyzer)
                title = lighter.getBestFragment(tokenstream, doc.get('title'))
                if title is None:
                    title = doc.get('title')
                if content is None:
                    content = doc.get('content')
                new = {}
                new['title'] = title
                new['media'] = doc.get('media')
                new['url'] = doc.get('news_url')
                new['pub_time'] = doc.get('pub_time')
                new['content'] = content
                new['picture_url'] = doc.get('first_img_url')
                new['tags'] = doc.get('tags')
                new['score'] = score
                new['news_id'] = doc.get('news_id')
                if new['picture_url'] == 'None':
                    new['picture_url'] = ""
                news_list += [new]
            indexreader.close()
            print("Searching End!")
            news = {}
            news['total'] = total
            news['news_list'] = news_list
            news['message'] = "Success"
            return news
        except Exception as error:
            print(error)
            news = {}
            news['total'] = 0
            news['news_list'] = []
            news['message'] = "Error"
            return news


def get_location(info_str, start_tag='<span class="szz-type">', end_tag='</span>'):
    """
    summary: pass in str
    Returns:
        location_list
    """

    start = len(start_tag)
    end = len(end_tag)
    location_infos = []
    pattern = start_tag + '(.+?)' + end_tag

    for idx, m_res in enumerate(re.finditer(r'{i}'.format(i=pattern), info_str)):
        location_info = []

        if idx == 0:
            location_info.append(m_res.span()[0])
            location_info.append(m_res.span()[1] - (idx + 1) * (start + end))
        else:
            location_info.append(m_res.span()[0] - idx * (start + end))
            location_info.append(m_res.span()[1] - (idx + 1) * (start + end))

        location_infos.append(location_info)

    return location_infos


class MyThread(Thread):
    """_summary_

    Args:
        Thread (_type_): build my thread
    """
    def __init__(self, func, args):
        super().__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        """_summary_

        Returns:
            _type_: get return result
        """
        try:
            return self.result
        except Exception:
            return None


if __name__ == "__main__":
    dispatcher = RPCDispatcher()
    transport = WsgiServerTransport(queue_class=gevent.queue.Queue)

    # start wsgi server as a background-greenlet
    wsgi_server = gevent.pywsgi.WSGIServer(('0.0.0.0', 5001), transport.handle)
    gevent.spawn(wsgi_server.serve_forever)

    rpc_server = RPCServerGreenlets(
        transport,
        JSONRPCProtocol(),
        dispatcher
    )

    mysearch = SearchEngine()
    print("Start")

    @dispatcher.public
    def search_news(keyword, page=0):
        """
        search interfer:
        """
        threads = []
        for i in range(1, 11):
            thread = MyThread(mysearch.search_news_thread, (keyword, "index/index"+str(i), page))
            thread.start()
            threads += [thread]
        for i in range(0, 10):
            threads[i].join()
        news_results = {}
        news_results['message'] = "Success"
        total = 0
        news_list = []
        for i in range(10):
            total += threads[i].get_result()['total']
            news_list += threads[i].get_result()['news_list']
        news_results['total'] = total
        news_results['news_list'] = news_list
        return news_results

    @dispatcher.public
    def search_keywords(keyword, must_contain=[], must_not=[], page=0):
        """
        search interfer:
        """
        return mysearch.search_keywords(keyword=keyword, must_contain=must_contain,
                                        must_not=must_not, page=page)

    @dispatcher.public
    def write_news(data_json):
        """
        write interfer:
        """
        return mysearch.add_news(data_json=data_json)

    @dispatcher.public
    def read_from_db():
        """
        read interfer:
        """
        return mysearch.read_from_db()

    @dispatcher.public
    def test_connection():
        """
        just for test
        """
        return "Success"

    rpc_server.serve_forever()
