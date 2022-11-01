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