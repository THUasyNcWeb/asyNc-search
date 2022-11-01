import lucene
import json
import sys,os
from java.io import File
from java.nio.file import Paths
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField, StringField
from org.apache.lucene.index import IndexWriter, Term, IndexWriterConfig, DirectoryReader 
from org.apache.lucene.store import Directory, FSDirectory
from org.apache.lucene.analysis.cn.smart import SmartChineseAnalyzer
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, Query, ScoreDoc, TopDocs
from org.apache.lucene.search.highlight import Highlighter, QueryScorer, SimpleFragmenter, SimpleHTMLFormatter, Fragmenter, TokenSources, SimpleSpanFragmenter

lucene.initVM()

analyzer = SmartChineseAnalyzer()
indexConfig = IndexWriterConfig(analyzer)

path = File("index")
file_path ="index"
directory = FSDirectory.open(Paths.get(file_path))
indexWriter = IndexWriter(directory, indexConfig)

def getDocument(data_json):
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

def add_news(data_json,file_path="index"):
    if os.path.exists(file_path) == False:
        os.mkdir(file_path)
    analyzer = SmartChineseAnalyzer()
    indexConfig = IndexWriterConfig(analyzer)
    directory = FSDirectory.open(Paths.get(file_path))
    indexWriter = IndexWriter(directory, indexConfig)
    document = getDocument(data_json)
    term = Term("news_url",data_json['news_url'])
    indexWriter.updateDocument(term,document)
    indexWriter.close()
    
# 参数一:默认的搜索域, 参数二:使用的分析器
queryParser = QueryParser("content", analyzer)
# 2.2 使用查询解析器对象, 实例化Query对象
query = queryParser.parse("content:元宇宙展")
indexReader = DirectoryReader.open(directory)
searcher = IndexSearcher(indexReader)
topDocs = searcher.search(query, 10)
print(topDocs.totalHits)
scoreDocs = topDocs.scoreDocs
# lighter.setTextFragmenter(fragmenter)
for scoreDoc in scoreDocs:
    docId = scoreDoc.doc
    score = scoreDoc.score
    doc = searcher.doc(docId)
    print(doc.get('title'))
    print(doc.get('media'))
    break
indexReader.close()