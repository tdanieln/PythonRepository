# -*- coding:utf-8 -*-
from bs4 import BeautifulSoup
from httpUtil import gethtml
__author__ = 'TangNan'

def parse_home_item(item):
    name = None
    stock = None
    min_amount = None
    href = None
    dict = {}
    tag_e_name_list = item.select('.e-name')
    if tag_e_name_list is not None and len(tag_e_name_list) > 0:
        name = tag_e_name_list[0].string
        href = tag_e_name_list[0].attrs.get('href')
    tag_num_list = item.select('.e-nums')
    if tag_num_list is not None and len(tag_num_list) > 0:
        tag_num = tag_num_list[0]
        tag_stock_list = tag_num.select('.c-4')
        tag_min_amount_list = tag_num.select('.c-2')
        if tag_stock_list is not None and len(tag_stock_list) > 0:
            tag_stock = tag_stock_list[0]
            stock = tag_stock.string
        if tag_min_amount_list is not None and len(tag_min_amount_list) > 0:
            tag_min_amount = tag_min_amount_list[0]
            min_amount = tag_min_amount.string
    if name is not None:
        dict['Name'] = name
    if stock is not None:
        dict['Stock'] = stock
    if min_amount is not None:
        dict['MinAmount'] = min_amount
    if href is not None:
        dict['Href'] = href
    return dict

def parseHtmlBybs4(decodeData):
    soup = BeautifulSoup(decodeData,"html.parser")
    items = soup.select('.dota-item.js-add-cart-parent')
    list = []
    for item in items:
        dict = parse_home_item(item)
        list.append(dict)
    print(list)
    return list

def get_detail_page(base_url, dict):
    href = dict.get('Href')
    ret_url = None
    if href is not None:
        ret_url = base_url  + href
    return ret_url

def parse_detail_page(url):
    content_str = gethtml(url)

# file_handler = open(r'd:/test.html','rb')
# content_byte = file_handler.read()
# file_handler.close()
# content_str = content_byte.decode('utf-8')
base_url = 'http://www.igxe.cn'
content_str = gethtml(base_url + '/dota2')
list_dict = parseHtmlBybs4(content_str)
dic = list_dict[0]
new_url = get_detail_page(base_url, dic)
print(new_url)
content_str = gethtml(new_url)
# print(content_str)