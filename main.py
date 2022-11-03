import lucene
import json
import sys,os
import zmq
import psycopg2

from tinyrpc.server import RPCServer
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.zmq import ZmqServerTransport
import re
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StringField, StoredField
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.index import IndexWriter, Term, IndexWriterConfig, DirectoryReader 
from org.apache.lucene.store import Directory, FSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser, QueryParserBase
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopDocs
from org.apache.lucene.search.highlight import Highlighter, QueryScorer, SimpleFragmenter, SimpleHTMLFormatter, Fragmenter, TokenSources, SimpleSpanFragmenter
from org.apache.lucene.search import BooleanQuery, TermQuery, BooleanClause
class search_engine(object):
    
    def __init__(self):
        """
        init lucene and database
        """
        lucene.initVM()
        self.analyzer = StandardAnalyzer()
        self.indexConfig = IndexWriterConfig(self.analyzer)
        '''
        Connect to the database
        '''
        with open('config.json', 'r', encoding='utf-8') as file:
            config = json.load(file)
        self.postgres = (config['hostname'], config['port'],
                         config['username'], config['password'],
                         config['database'])
        self.connection = psycopg2.connect(
            host=self.postgres[0], port=self.postgres[1],
            user=self.postgres[2], password=self.postgres[3],
            dbname=self.postgres[4])
        self.cur = self.connection.cursor()
        
    def check_db_status(self):
        query = r'select count(*) from news;'
        self.cur.execute(query)
        result = self.cur.fetchall()
        print(result)
    
    def read_from_db(self):
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
            if data['first_img_url'] == None:
                data['first_img_url'] = "None"
            data['pub_time'] = str(result[9])
            # data['news_id'] = str(result[0])
            data['title'] = result[5]
            data['content'] = result[7]
            self.add_news(data)
        return len(result)
        
    def getDocument(self, data_json):
        document = Document()
        # 给文档对象添加域
        # add方法: 把域添加到文档对象中, field参数: 要添加的域
        # TextField: 文本域, 属性name:域的名称, value:域的值, store:指定是否将域值保存到文档中
        # StringField: 不分词, 作为一个整体进行索引
        if data_json['first_img_url'] == None or data_json['first_img_url'] == '': 
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
    
    def add_news(self,data_json,file_path="index"):
        if os.path.exists(file_path) == False:
            os.mkdir(file_path)
        try:
            data_json = json.dumps(data_json)
            data_json = json.loads(data_json, strict=False)
        except ValueError:
            return False
        try:
            analyzer = self.analyzer
            indexConfig = IndexWriterConfig(analyzer)
            directory = FSDirectory.open(Paths.get(file_path))
            indexWriter = IndexWriter(directory, indexConfig)
            document = self.getDocument(data_json)
            term = Term("news_url",data_json['news_url'])
            indexWriter.updateDocument(term,document)
            indexWriter.close()
            return True
        except:
            return False
        
    def search_news(self,keyword,file_path='index'):
        try:
            # 参数一:默认的搜索域, 参数二:使用的分析器
            queryParser = QueryParser("content", self.analyzer)
            # 2.2 使用查询解析器对象, 实例化Query对象
            search_content = "content:"+str(keyword)
            query = queryParser.parse(search_content)
            directory = FSDirectory.open(Paths.get(file_path))
            indexReader = DirectoryReader.open(directory)
            searcher = IndexSearcher(indexReader)
            topDocs = searcher.search(query, 10)
            # print(topDocs.totalHits)
            scoreDocs = topDocs.scoreDocs
            qs = QueryScorer(query)
            simpleHTMLFormatter = SimpleHTMLFormatter('<span class="szz-type">', '</span>')
            lighter = Highlighter(simpleHTMLFormatter,qs)
            news_list = []
            # lighter.setTextFragmenter(fragmenter)
            for scoreDoc in scoreDocs:
                docId = scoreDoc.doc
                score = scoreDoc.score
                doc = searcher.doc(docId)
                tokenStream = TokenSources.getTokenStream(doc, "content", self.analyzer)
                content = lighter.getBestFragment(tokenStream, doc.get('content'))
            indexReader.close()
            return content
        except:
            return False

if __name__ == "__main__":
    ctx = zmq.Context()
    dispatcher = RPCDispatcher()
    transport = ZmqServerTransport.create(ctx, 'tcp://127.0.0.1:5001')

    rpc_server = RPCServer(
        transport,
        JSONRPCProtocol(),
        dispatcher
    )
    mysearch =search_engine()
    @dispatcher.public
    def search_news(keyword):
        return mysearch.search_news(keyword=keyword)
    # search.search_news()
    @dispatcher.public
    def write_news(data_json):
        return mysearch.add_news(data_json=data_json)
    rpc_server.serve_forever()