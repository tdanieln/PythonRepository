# -*- coding:utf-8 -*-
import psutil
import meminfo
__author__ = 'TangNan'

# CPU信息部分

# *********************psutil.cpu_times(percpu=False)

# 返回信息与平台有关，类型为tuple
# user: 执行用户进程的时间消耗
# system：执行kernal进程的时间消耗
# idle：空闲时间
# Linux专有↓↓↓↓↓↓↓↓
# iowait: 花费在等待IO完成上的时间
# irq：花费在硬件中断上的时间
# softirq: 花费在软件中断上的时间
# steal：虚拟其他操作系统所花费的时间
# windows↓↓↓↓
# interrupt：类似linux irq，等待硬件中断时间
# dpc：花费在dpc上的时间，优先级比中断要低
#
cpu_info = psutil.cpu_times()
print(cpu_info)
# print(cpu_info.user)

# 如果带了参数，返回的就是一个list，里边是每一个逻辑CPU的信息
cpu_infos = psutil.cpu_times(True)
# print(cpu_infos)
# for eachcpu in cpu_infos:
#     print(eachcpu)

# ******************psutil.cpu_percent(interval=None, percpu=False)
# 返回CPU使用率，interval一般最小填0.1，如果填0或者null，那么至少需要执行2次，第一次相当于启动秒表，第二次相当于停止秒表
cpu_percent = psutil.cpu_percent(interval=0.1)
print('CPU整体使用率为%s'%cpu_percent)

cpu_percents = psutil.cpu_percent(interval=0.1, percpu=True)

print('每个逻辑CPU的使用率为' + ' '.join(str(e) for e in cpu_percents))


# ****************psutil.cpu_count(logical=True)
# 返回CPU逻辑个数，如果参数为True，则返回逻辑CPU个数；如果参数为False，那么返回物理核心数。返回数量也可能是None
psutil.cpu_count(logical=False)
psutil.cpu_count(True)



# ***************psutil.cpu_freq(percpu=False)
# 显示cpu的频率信息，如果参数为True，则显示每个逻辑CPU的频率信息(带参数的话的话，仅对linux有效)
#cpu_freq = psutil.cpu_freq(percpu=False)
#print('CPU当前频率为%d,最低频率为%d,最高频率为%d'%(cpu_freq.current, cpu_freq.min, cpu_freq.max))
#every_cpu_freq = psutil.cpu_freq(percpu=True)


# 内存信息部分

# ***************psutil.virtual_memory()
# 返回系统内存的统计信息
# total：总的物理内存
# available：系统可以不使用交换区内存，而直接可以调用的内存信息。
#
virtual_memory = psutil.virtual_memory()
print('内存总量为%s,可用内存为%s,已使用率为%d%%'\
      %(meminfo.bytes2human(virtual_memory.total), \
        meminfo.bytes2human(virtual_memory.available),\
        virtual_memory.percent))

# **************psutil.swap_memory()
# 返回交换空间的统计信息
swap_memory = psutil.swap_memory()
print('交换空间总量为%s，已使用交换空间为%s，剩余交换空间为%s，使用率为%d%%，累计从磁盘获取数据%s，累计向磁盘交换数据%s'\
      %(meminfo.bytes2human(swap_memory.total), meminfo.bytes2human(swap_memory.used),\
        meminfo.bytes2human(swap_memory.free), swap_memory.percent, meminfo.bytes2human(swap_memory.sin),\
        meminfo.bytes2human(swap_memory.sout)))


# 磁盘信息部分
# *************psutil.disk_partitions(all=False)
# 返回一个list，每个元素包含设备，挂载点，文件系统，类似于linux下df命令
# 如果参数是false，那么命令将试图找出物理设备，而忽略其他设备
disk_partitions_list = psutil.disk_partitions()
disk_partitions_list = psutil.disk_partitions(True)

disk_usage = psutil.disk_usage(disk_partitions_list[0].mountpoint)

# *************psutil.disk_usage(path)
# 返回磁盘分区的使用统计信息，用bype方式表示
for disk_partition in disk_partitions_list:
    try:
        disk_usage = psutil.disk_usage(disk_partition.mountpoint)
        print('分区%s总计空间为：%s,已使用空间为%s，可使用空间为%s，使用率为%d%%'\
              %(disk_partition.mountpoint, meminfo.bytes2human(disk_usage.total), meminfo.bytes2human(disk_usage.used), \
                meminfo.bytes2human(disk_usage.free), disk_usage.percent))
    except Exception as e:
        print(e)













