#coding:utf8
from urllib.error import URLError,HTTPError
import urllib.request
import http.cookiejar
from bs4 import BeautifulSoup


__author__ = 'TangNan'

#获取HTML信息
def gethtml(url,encode='UTF-8'
            ,user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'):
    #设置header内容
    header = assembly_http_header()
    #设置cookie内容
    cj = http.cookiejar.CookieJar()
    pro = urllib.request.HTTPCookieProcessor(cj)
    opener = urllib.request.build_opener(pro)
    #设置发送的data内容
    data = '';
    #产生request的实例
    request = urllib.request.Request(url,headers=header)

    #打开url，获得response
    try:
        urllib.request.install_opener(opener)
        response = urllib.request.urlopen(request)
    except HTTPError as e:
        print('HttpError',e.code)
    except URLError as e:
        print('URLError',e.reason)
        #从response中获取html的内容
    # 获得content type类型
    contype = response.headers['Content-Type']
    # 获取字符集
    charset = parse_charset(contype)
    print(charset)
    html_content_byte = response.read()
    #解码
    # decodeData = htmlContent.decode(contype,'ignore');
    # html_content = trans_to_gbk(html_content_byte, charset)
    html_content = trans_to_utf8(html_content_byte, charset)
    return html_content;

def assembly_http_header():
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    dict = {}
    dict['User-Agent'] = user_agent
    return dict

def parse_charset(content_type):
    charset = 'ISO8859-1'
    if content_type is None:
        pass
    else:
        pos = content_type.find('=')
        if pos != -1:
            charset = content_type[pos+1:len(content_type)]
        else:
            pass
    return charset


def trans_to_utf8(html_bytes, charset):
    if charset == 'utf-8':
        html_utf8_str = html_bytes.decode(charset, 'ignore')
    else:
        # 如果不是utf8编码，先把byte型的html文件按照相应的解码成相应的Unicode码
        html_unicode = html_bytes.decode(charset, 'ignore')
        # 使用utf-8编码
        html_utf8_byte = html_unicode.encode('utf-8','ignore')
        # 使用utf-8解码
        html_utf8_str = html_utf8_byte.decode('utf-8','ignore')
    return html_utf8_str

def trans_to_gbk(html_bytes, charset):
    if charset == 'GBK':
        html_gbk_str = html_bytes.decode(charset, 'ignore')
    else:
        html_unicode = html_bytes.decode(charset, 'ignore')
        html_gbk_byte = html_unicode.encode('GBK','ignore')
        html_gbk_str = html_gbk_byte.decode('GBK','ignore')
    return html_gbk_str


#通过bs4解析html的内容
def parseHtmlBybs4(decodeData):
    soup = BeautifulSoup(decodeData,"html.parser")
    print('获取所有的链接')
    links = soup.find_all('a')
    for link in links:
        print(link.name , link['href'] )
    return