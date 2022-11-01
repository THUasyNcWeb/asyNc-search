import lucene
import json
import sys,os
import zmq

from tinyrpc.server import RPCServer
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.zmq import ZmqServerTransport
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StringField
from org.apache.lucene.index import IndexWriter, Term, IndexWriterConfig, DirectoryReader 
from org.apache.lucene.store import Directory, FSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopDocs
from org.apache.lucene.search.highlight import Highlighter, QueryScorer, SimpleFragmenter, SimpleHTMLFormatter, Fragmenter, TokenSources, SimpleSpanFragmenter

class search_engine(object):
    
    def __init__(self):
        lucene.initVM()
        self.analyzer = StandardAnalyzer()
        self.indexConfig = IndexWriterConfig(self.analyzer)
        
    def getDocument(self, data_json):
        document = Document()
        # 给文档对象添加域
        # add方法: 把域添加到文档对象中, field参数: 要添加的域
        # TextField: 文本域, 属性name:域的名称, value:域的值, store:指定是否将域值保存到文档中
        # StringField: 不分词, 作为一个整体进行索引
        document.add(StringField("news_url", data_json['news_url'], Field.Store.YES))
        document.add(TextField("title", data_json['title'], Field.Store.YES))
        document.add(TextField("content", data_json['content'], Field.Store.YES))
        document.add(TextField("media", data_json['media'], Field.Store.YES))
        document.add(TextField("category", str(data_json['category']), Field.Store.YES))
        return document
    
    def add_news(self,data_json,file_path="index"):
        if os.path.exists(file_path) == False:
            os.mkdir(file_path)
        data_json = json.loads(data_json, strict=False)
        # print(data_json)
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
            # lighter.setTextFragmenter(fragmenter)
            for scoreDoc in scoreDocs:
                docId = scoreDoc.doc
                score = scoreDoc.score
                doc = searcher.doc(docId)
                tokenStream = TokenSources.getTokenStream(doc, "content", self.analyzer)
                content = lighter.getBestFragment(tokenStream, doc.get('content'))
                # print(content)
                print(doc.get('title'))
                # print(doc.get('media'))
                break
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