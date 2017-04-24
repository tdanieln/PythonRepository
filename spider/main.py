# -*- coding:utf-8 -*-

from httpUtil import gethtml
from httpUtil import parseHtmlBybs4
import locale

s = gethtml("http://www.sohu.com.cn")
print(s)
# # print(locale.getdefaultlocale())
# print(s)
# # print(isinstance(s, 'unicode'))
# s1 = s.decode('UTF-8', 'ignore')
#
# s2 = s1.encode('GBK', 'ignore')
# s3 = s2.decode('GBK')
# print(s3)
# parseHtmlBybs4(s1);

# str='呵呵'
# print(str)
# gbk_encode_byte = str.encode('GBK')
# utf8_encode_byte = str.encode('UTF-8')
# print(gbk_encode_byte)
# print(utf8_encode_byte)
# # b2 = gbk_str.encode('UTF-8')
# print(b2)
# print(type(b2))
# s3 = b2.decode('UTF-8')
# print(s3)








