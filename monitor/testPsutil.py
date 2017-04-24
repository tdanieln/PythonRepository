# -*- coding:utf-8 -*-
import psutil

__author__ = 'TangNan'

# CPU信息部分
# psutil.cpu_times(percpu=False)
# 返回信息与平台有关
cpu_info = psutil.cpu_times()
print(type(cpu_info))
print(cpu_info)


