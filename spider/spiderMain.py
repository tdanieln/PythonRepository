__author__ = 'TangNan'
import urllib.request
import http.cookiejar
from urllib.error import URLError,HTTPError
from bs4 import BeautifulSoup
import re
import urllib.parse


class SpiderMain(object):
    def __init__(self):
        self.urlManager = URLManager()
        self.httpDownloader = HttpDownloader()
        self.httpParser = HttpParser()

    def crawl(self,root_url):
        pass


class URLManager(object):
    def __init__(self):
        # ���ڴ洢�����ʵ�URL
        self.new_url = set()
        # ���ڴ洢�ѷ��ʵ�URL
        self.old_url = set()

    def add_new_url(self,url):
        if url is None:
            return
        if url not in self.new_url and url not in self.old_url:
            self.new_url.add(url)

    def add_many_urls(self,urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    def has_new_url(self):
        return len(self.new_url) != 0

    def get_new_url(self):
        new_url = self.new_url.pop()
        self.old_url.add(new_url)
        return new_url


class HttpDownloader(object):

    def download(self,url):
        if url is None:
            return None
        return

    #��ȡHTML��Ϣ
    def gethtml(url,encode='UTF-8'
                ,user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'):
        #����header����
        header = {'User-Agent':user_agent}
        #����cookie����
        cj = http.cookiejar.CookieJar()
        pro = urllib.request.HTTPCookieProcessor(cj)
        opener = urllib.request.build_opener(pro)
        #���÷��͵�data����
        data = '';
        #����request��ʵ��
        request = urllib.request.Request(url,headers=header)

        #��url�����response
        try:
            urllib.request.install_opener(opener)
            response = urllib.request.urlopen(request)
        except HTTPError as e:
            print('HttpError',e.code)
        except URLError as e:
            print('URLError',e.reason)
            #��response�л�ȡhtml������

        htmlContent= response.read()
        #����
        decodeData = htmlContent.decode(encode,'ignore');
        return decodeData;

#
class HttpParser(object):
    def _get_new_urls(self,page_url,soup):
        new_urls = set()
        links = soup.find_all('a',herf=re.compile(r'/view\d+\.htm'))
        for link in links:
            new_url = link['herf']
            new_full_url = urllib.parse.urljoin(page_url,new_url)
            new_urls.add(new_full_url)

    def _get_new_data(self,page_url,soup):
        res_data = {}
        title_node = soup.find('dd',class_="lemmaWgt-lemmaTitle-title").find("h1")
        res_data['title'] = title_node.get_text()


    def parse(self,page_url,html_cont):
        if page_url is None or html_cont is None:
            return
        soup = BeautifulSoup(html_cont,'html.parser',from_encoding='utf-8')
