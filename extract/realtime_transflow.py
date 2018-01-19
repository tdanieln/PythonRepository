# -*- coding: utf-8 -*-
import ftplib
import os
import logging
import re
import datetime
import time
import pymysql
import sys
import shutil
import redis
from ftplib import FTP
from datetime import date

__author__ = 'TangNan  tangnan@cib.com.cn /tdanieln@gmail.com'


# 列属性
class ColumnInfo:
    def __init__(self, order, column_name, column_type, start_position, end_position, column_length
                 , decimal_length=0, decimal_point_length=0):
        #
        """数据列信息，决定了定长数据中的解析顺序，与生成insert语句的内容，极为重要
        :param order: 顺序号
        :param column_name: 列名称
        :param column_type: 列属性
        :param start_position: 定长报文中的开始位置
        :param end_position: 定长报文中的的结束位置
        :param column_length: 列宽度
        :param decimal_length: 如果是deicmal类型，decimal类型的长度
        :param decimal_point_length: 如果是deicmal类型，小数点后的长度
        :return:
        """
        self.order = order
        self.column_name = column_name
        self.column_type = column_type
        self.column_length = column_length
        self.start_position = start_position
        self.end_position = end_position
        self.decimal_length = decimal_length
        self.decimal_point_length = decimal_point_length


# 根据flg文件或者其他方式得到的数据结构
class DataInfo:
    def __init__(self, file_name, row_count, row_length, column_count, list_column, table_name=""):
        #
        """
        构造函数
        :param file_name: 数据文件名
        :param row_count: 数据行数
        :param row_length: 每行数据长度
        :param column_count: 数据列数
        :param list_column: column对象组成的list
        :return:
        """
        self.file_name = file_name
        if table_name is None or len(table_name) == 0:
            self.table_name = file_name.split(r'.')[0]
        self.row_count = row_count
        self.row_length = row_length
        self.column_count = column_count
        self.list_column = list_column

    # 根据结构生成建表语句
    def generate_default_create_table_sql(self):
        if self.list_column is None or len(self.list_column) == 0:
            logger.error("生成对象为Null或者列属性为空")
            raise Exception("生成对象为Null或者列属性为空");

        if self.table_name is None or len(self.table_name) == 0:
            logger.error("生成对象为Null或者表名为空")
            raise Exception("生成对象为Null或者表名为空");

        list_column = self.list_column
        table_name = self.table_name
        struct_sql = ""
        # 循环列属性
        for column in list_column:
            column_sql = ""
            # CHAR和char的处理
            if column.column_type == "char" or column.column_type == "CHAR":
                if column.column_length < 60:
                    column_sql = column.column_name + " CHAR(" + str(column.column_length) + "),"
                else:
                    column_sql = column.column_name + " VARCHAR(" + str(column.column_length) + "),"
            elif column.column_type == "varchar" or column.column_type == "VARCHAR":
                column_sql = column.column_name + " VARCHAR(" + str(column.column_length) + "),"
            # DECIMAL和decimal的处理
            elif column.column_type == "decimal" or column.column_type == "DECIMAL":
                column_sql = column.column_name + " DECIMAL(" \
                             + str(column.decimal_length) + "," + str(column.decimal_point_length) + "),"
            # date和Date的处理，由于各个dbms实现和定义不一样，所以需要特别注意
            elif column.column_type == "date" or column.column_type == "DATE":
                if column.column_length == 8:
                    column_sql = column.column_name + " DATE,"
                elif column.column_length == 16:
                    column_sql = column.column_name + " DATETIME,"
            # smallint和SMALLINT的处理
            elif column.column_type == "smallint" or column.column_type == "SMALLINT":
                column_sql = column.column_name + " SMALLINT,"
            # INT的处理
            elif column.column_type == "int" or column.column_type == "INT":
                column_sql = column.column_name + " INTEGER,"
            # 对于Informix专用时间戳的处理
            elif column.column_type == "datetime year to fraction":
                column_sql = column.column_name + " CHAR(" + str(column.column_length) + "),"
            elif column.column_type == "serial8":
                column_sql = column.column_name + " CHAR(" + str(column.column_length) + "),"
            else:
                raise Exception("有未处理的列信息!!!!!其字段类型为%s" % column.column_type)
            struct_sql += column_sql
        struct_sql = struct_sql[0:len(struct_sql) - 1]

        sql = "create table " + table_name + "(" + struct_sql + ")"
        return sql


# mysql连接串信息
class MysqlConnect:
    def __init__(self, host='127.0.0.1', port=3306, user='mysql', passwd='mysql', db='etldb', charset='utf8'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        pass


class FtpConnect:
    def __init__(self, host='127.0.0.1', port=21, user='', passwd='', directory=''):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.directory = directory


# 获取logger对象
def ini_logger():
    logging.basicConfig(level=logging.DEBUG
                        , format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        filename='into_db.log', filemode='a')
    return logging


# 生成insert语句的声明部分
def gen_insert_station_partition(list_column_info, table_name):
    station_sql = "insert into " + table_name + " ("
    value_sql = "("
    for column_info in list_column_info:
        station_sql += column_info.column_name + ","
        value_sql += "%s,"
    station_sql = station_sql[0:len(station_sql) - 1] + ") values "
    value_sql = value_sql[0:len(value_sql) - 1] + ")"
    return station_sql + value_sql


# 如果控制表不存在，则创建新表
def create_trans_flow_control_table(cursor):
    print(type(cursor))

    if cursor is None:
        raise Exception('游标不存在，无法初始化控制表')
    if isinstance(cursor, pymysql.cursors.Cursor) is False:
        raise Exception('连接不为pymysql类型，无法创建数据表')
    # 建表语句
    create_ctrl_sql = "create table realtime_transflow_ctrl_flow (\
                      trans_date DATE,file_name varchar(100),row_count int,status int)";
    cursor.execute(create_ctrl_sql)
    logger.debug('创建控制表成功，sql语句为%s' % create_ctrl_sql)


# 如果结构表不存在，则创建新表
def create_table_column_table(cursor):
    if cursor is None:
        raise Exception('连接不存在，无法初始化控制表')
    if isinstance(cursor, pymysql.cursors.Cursor) is False:
        raise Exception('连接不为pymysql类型，无法创建数据表')
    # 建表语句
    create_table_column_sql = "create table table_column_inf (" \
                              " table_name varchar(100),column_name varchar(50),column_type varchar(30),column_order smallint" \
                              " ,length smallint,status smallint) "
    cursor.execute(create_table_column_sql)
    logger.debug('创建表结构信息表成功，sql语句为%s' % create_table_column_sql)


# 初始化流程
def initial_conf(mysql_connect):
    try:
        is_first_run = False
        mysql_conn = pymysql.connect(host=mysql_connect.host, port=mysql_connect.port, user=mysql_connect.user \
                                     , passwd=mysql_connect.passwd, db=mysql_connect.db, charset=mysql_connect.charset)
        cursor = mysql_conn.cursor()
    except Exception as e:
        print(type(e))
        print(e)
        logger.error('连接目标数据库失败，请检查连接参数的信息')
        raise Exception('连接目标数据库失败，请检查连接参数的信息')

    try:
        cursor.execute('select * from realtime_transflow_ctrl_flow limit 0,1')
    # 捕获程序异常
    except pymysql.err.ProgrammingError as e:
        logger.debug('控制表信息不存在，将创建控制表')
        try:
            create_trans_flow_control_table(cursor)
            is_first_run = True
        except Exception as ee:
            print(type(ee))
            raise Exception('创建状态控制表失败,无法正常启动')

    try:
        cursor.execute('select * from table_column_inf limit 0,1')
    # 捕获程序异常
    except pymysql.err.ProgrammingError as e:
        logger.debug('表结构信息表不存在，将创建控制表')
        try:
            create_table_column_table(cursor)
            is_first_run = True
        except Exception as ee:
            print(type(ee))
            raise Exception('表结构信息表不存在,无法正常启动')
    cursor.close()
    if is_first_run:
        raise Exception('初次运行程序，初始化数据表完成，请重新执行本程序')
    return mysql_conn


# 下载文件
def get_ftp_files(host, user, passwd, remote_dir, local_base_dir, pattern, timestamp, conn):
    try:
        # ftp对象与登录信息
        ftp = FTP()
        ftp.connect(host=host)
        ftp.login(user=user, passwd=passwd)
        ftp.cwd(remote_dir)
        logger.debug("打开ftp:%s,用户名:%s,目录:%s成功" % (host, user, remote_dir))
        # 获取当日时间戳
        deal_date = timestamp
        # 获取yyyyMMdd的样式
        deal_date_ymd = deal_date.strftime('%Y%m%d')
        # 如果目录不存在，则创建
        local_dir = os.path.join(local_base_dir, deal_date_ymd)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # 设定过滤器
        if pattern is None or len(pattern) == 0:
            pattern = 'ZLCPWJ' + deal_date_ymd + r'\d{8}\.32\.ok'

        file_name_pattern = re.compile(pattern)
        # 获取远端文件list
        # remote_names = ftp.nlst()
        remote_names = replace_nlst(ftp,pattern)
        # 获得可下载列表
        could_down_file_list = []
        for ok_file_name in remote_names:
            # 如果符合过滤器条件，则添加进可下载列表中
            if file_name_pattern.match(ok_file_name):
                z_file_name = ok_file_name.replace('ok', 'Z')
                # 只有z文件再可下载列表里时，才能加入可下载列表
                if z_file_name in remote_names:
                    could_down_file_list.append(z_file_name)
            else:
                pass

        cursor = conn.cursor()
        # 形成真·下载文件
        download_file_list = get_to_download_file_list(could_down_file_list, cursor)

        # 真的下载在这里
        download_file(ftp, local_dir, download_file_list, timestamp, cursor)

        conn.commit()
    except ftplib.all_errors as ftperr:
        logger.error("ftp相关错误")
        logger.error(ftperr)
        pass
    finally:
        ftp.quit()


# 筛选可下载列表中的文件，形成真·下载列表
def get_to_download_file_list(could_down_file_list, cursor):
    download_file_list = []
    query_sql = "select row_count from realtime_transflow_ctrl_flow where file_name=%s"
    for file_name in could_down_file_list:
        cursor.execute(query_sql, (file_name,))
        # 只要找到记录就认为已经下载过
        row = cursor.fetchone();
        if row is None or len(row) == 0:
            # 没找到记录就添加进真·下载列表
            download_file_list.append(file_name)
    return download_file_list


# 真正的下载文件
def download_file(ftp, local_dir, download_file_list, timestamp, cursor):
    # 如果没有需要下载的内容，就GG
    if download_file_list is None or len(download_file_list) == 0:
        return

    # 获取inset到db的时间戳
    deal_date_ymd = timestamp.strftime('%Y%m%d')
    agrs = []
    ins_sql = "insert into realtime_transflow_ctrl_flow (trans_date,file_name,row_count,status) values (%s,%s,%s,%s)"
    for file_name in download_file_list:
        local_file = os.path.join(local_dir, file_name)
        with open(local_file, mode='wb') as f:
            ftp.retrbinary('RETR ' + file_name, f.write, 1024)
            logger.debug(file_name + "完成下载")
        row_data = [deal_date_ymd, file_name, 0, '2']
        agrs.append(row_data)
    cursor.executemany(ins_sql, agrs)

# 用于替代sb总行ftp服务器不支持的nlst命令
def replace_nlst(ftp,pattern):
    ftp_file_name_list = []
    ftp.retrlines('LIST',ftp_file_name_list.append)
    ret_file_list = []
    pattern = r'ZLCPWJ\d*\d{8}\.32\.(ok|Z)'
    prog = re.compile(pattern)
    for file_name in ftp_file_name_list:
        match = prog.search(file_name)
        if match is None:
            continue
        else:
            ret_file_list.append(match.group())

    return ret_file_list



def decompress_Z_file(local_dir, timestamp, conn):
    cursor = conn.cursor()
    deal_date_ymd = timestamp.strftime('%Y%m%d')
    query_will_import_data = 'select file_name from realtime_transflow_ctrl_flow ' \
                             ' where status=2 and trans_date=%s'
    update_import_data = 'update realtime_transflow_ctrl_flow set status=3 where ' \
                         ' file_name=%s and trans_date=%s and status=2'
    del_import_date = 'delete from realtime_transflow_ctrl_flow where file_name=%s and status=2'

    cursor.execute(query_will_import_data, (deal_date_ymd,))
    rows = cursor.fetchall()
    # 如果找到了已下载，但是还没处理的数据
    if rows is not None:
        for row in rows:
            z_file_name = row[0]
            z_file_abs_path = os.path.join(local_dir, deal_date_ymd, z_file_name)
            z_decompress_file_name = z_file_abs_path.replace('.Z', '')
            try:
                if not os.path.exists(z_file_abs_path):
                    raise Exception('文件不存在!')
                if os.path.exists(z_decompress_file_name):
                    os.remove(z_decompress_file_name)
                command = 'gzip -d %s' % z_file_abs_path
                ret_value = os.system(command)
                if ret_value == 0:
                    cursor.execute(update_import_data, (z_file_name, deal_date_ymd,))
                else:
                    raise Exception('解压缩文件失败')
            # 无论是文件没找到还是解压缩失败，都干掉已下载的记录，让程序去重新下载
            except Exception as e:
                logger.error(e)
                cursor.execute(del_import_date, (z_file_name,))
                pass
    conn.commit()


# 解析db中的列文件
def parse_column_info_from_db(conn):
    c = conn.cursor()
    data_info_sql = "select column_order,column_name,column_type,length" \
                    " from table_column_inf where table_name=%s and status=%s"
    args = ['realtime_transflow', 1]
    c.execute(data_info_sql, args)
    rows = c.fetchall()
    list_column = []
    decimal_length = 0
    decimal_point_length = 0
    for row in rows:
        column_list = []
        position = 0
        for member in row:
            if position == 0:
                column_list.append(member)
            elif position == 1:
                column_list.append(member)
            elif position == 2:
                content_type_content = member.split(r'(')
                column_list.append(content_type_content[0])
                if content_type_content[0] == "decimal" or content_type_content[0] == "DECIMAL":
                    content_type_value = content_type_content[1].strip(r')').split(r',')
                    decimal_length = int(content_type_value[0])
                    decimal_point_length = int(content_type_value[1])
            else:
                pass
            position += 1
        # 如果不是decimal型，则按照普通类型
        if decimal_length is None or decimal_length == 0:
            c = ColumnInfo(column_list[0], column_list[1], column_list[2], 0, 0, 0)
        else:
            c = ColumnInfo(column_list[0], column_list[1], column_list[2], 0, 0 \
                           , 0, decimal_length, decimal_point_length)
        list_column.append(c)

    return list_column


def gen_insert_value_partition(str_data_content):
    # 需要去换行符和最后一个"|"
    str_data_content = str_data_content[0:(len(str_data_content) - 2)]
    data_element = str_data_content.split(r'|')
    ret_args = []
    for element in data_element:
        ret_args.append(element)
    return ret_args


def store_filedata_to_db(table_name, file, mysql_conn, r=None):
    mysql_cur = mysql_conn.cursor()

    list_column_info = parse_column_info_from_db(mysql_conn)
    station_sql = gen_insert_station_partition(list_column_info, table_name)
    total_count = 0

    redis_is_ok = False
    subject_public_current_set = set()
    subject_private_current_set = set()
    subject_time_deposit_set = set()
    subject_loan_set = set()
    subject_internal_set = set()
    subject_off_balance_set = set()
    try:
        if r is not None:
            subject_public_current_set_byte = r.smembers('subjecttype:0')
            subject_private_current_set_byte = r.smembers('subjecttype:1')
            subject_time_deposit_set_byte = r.smembers('subjecttype:2')
            subject_loan_set_byte = r.smembers('subjecttype:3')
            subject_internal_set_byte = r.smembers('subjecttype:4')
            subject_off_balance_set_byte = r.smembers('subjecttype:5')
            for d in subject_public_current_set_byte:
                subject_public_current_set.add(d.decode())
            for d in subject_private_current_set_byte:
                subject_private_current_set.add(d.decode())
            for d in subject_time_deposit_set_byte:
                subject_time_deposit_set.add(d.decode())
            for d in subject_internal_set_byte:
                subject_internal_set.add(d.decode())
            for d in subject_loan_set_byte:
                subject_loan_set.add(d.decode())
            for d in subject_off_balance_set_byte:
                subject_off_balance_set.add(d.decode())

            redis_is_ok = True
    except :
        redis_is_ok = False

    args_list = []
    count = 0
    with open(file, 'r', encoding='gbk') as f:
        for line in f:
            args = gen_insert_value_partition(line)
            args_list.append(args)
            try:
                if redis_is_ok is True:
                    i = 0
                    count = args.__len__()
                    dict = {}
                    while i < count:
                        dict[list_column_info[i].column_name] = args[i]
                        i += 1
                    subject_no = dict['kmdh']
                    debit_credit_signal = dict['jdbj']
                    account_no = dict['zhdh']
                    trans_date = dict['jyrq']
                    open_agency_no = dict['khjgdh']
                    open_area_no = dict['khdqdh']
                    currency_type = dict['hbzl']
                    customer_no = dict['khdh']
                    trans_amount = dict['jyje']
                    new_date = datetime.datetime.strptime(trans_date, '%Y/%m/%d') + datetime.timedelta(days=1)
                    now_date = datetime.datetime.strptime(trans_date, '%Y/%m/%d').date()
                    now_date_str = now_date.strftime('%Y%m%d')
                    # 只统计北京分行开的户
                    if open_area_no == '32':
                        # 先只统计对公活期账户的，用来做实验
                        if subject_no in subject_public_current_set:
                            account_type = '0'
                            key_long = now_date_str + ':' + open_agency_no + ':' + debit_credit_signal + ':' \
                                        + account_type + ':' + currency_type
                            key_short = now_date_str + ':' + debit_credit_signal + ':' + account_type + ':' + currency_type
                            key_short_customer = now_date_str + ':' + debit_credit_signal + ':' + 'customer' + ':' + currency_type
                            r.zincrby(key_long, account_no, round(float(trans_amount),2))
                            r.expireat(key_long, new_date)
                            r.zincrby(key_short, account_no, round(float(trans_amount),2))
                            r.expireat(key_short, new_date)
                            r.zincrby(key_short_customer, customer_no, round(float(trans_amount),2))
                            r.expireat(key_short_customer, new_date)
                    else:
                        pass


            except:
                pass

            count += 1
            total_count += 1
            if count == 3000:
                mysql_cur.executemany(station_sql, args_list)
                mysql_conn.commit()
                args_list = []
                count = 0
    #print(station_sql)
    #print(args_list) 
    mysql_cur.executemany(station_sql, args_list)
    mysql_conn.commit()
    return total_count

def ins_data_to_appdb(local_base_dir, timestamp, conn, r=None):
    cursor = conn.cursor()
    deal_date_ymd = timestamp.strftime('%Y%m%d')
    query_will_import_data = "select file_name from realtime_transflow_ctrl_flow " \
                             " where status=3 and trans_date=%s"
    update_import_data = "update realtime_transflow_ctrl_flow set status=1,row_count=%s where file_name=%s and trans_date=%s and status=3"

    cursor.execute(query_will_import_data, (deal_date_ymd,))
    rows = cursor.fetchall()
    if rows is not None:
        for row in rows:
            z_file_name = row[0]
            # 获得原来Z文件的路径
            z_file_path = os.path.join(local_base_dir, deal_date_ymd, z_file_name)
            # 替换为解压后的文件
            file_name = z_file_path.replace('.Z', '')
            # 数据入库
            count = store_filedata_to_db('realtime_transflow', file_name, conn, r)
            update_args = [count,z_file_name, deal_date_ymd, ]
            cursor.execute(update_import_data, update_args)
    pass


def del_past_data(local_base_dir, timestamp, conn):
    cursor = conn.cursor()
    deal_date_ymd = timestamp.strftime('%Y%m%d')
    logger.debug('调用删除%s数据表及文件模块'%deal_date_ymd)
    del_ctrl_sql = 'delete from realtime_transflow_ctrl_flow where trans_date=%s'
    del_detail_sql = 'delete from realtime_transflow where jyrq=%s'

    cursor.execute(del_detail_sql,(deal_date_ymd,))
    z_file_path = os.path.join(local_base_dir, deal_date_ymd)
    try:
        if len(z_file_path)>10:
            if os.path.exists(z_file_path):
                shutil.rmtree(z_file_path)
            cursor.execute(del_ctrl_sql,(deal_date_ymd,))
    except OSError as e:
        logger.error('删除目录文件失败')


def main():
    # 参数数量
    args_len = len(sys.argv)
    run_mode = 'dev'

    if args_len >=2:
        run_mode = sys.argv[1]

    if run_mode == 'product':
        print('使用生产模式运行')
        mysql_connect = MysqlConnect(host='168.32.12.63', port=3201, user='dbmnger', passwd='dbmnger')
        ftp_connect = FtpConnect(host='168.7.12.133', directory='./',user='cib32',passwd='cibbj32')
        try:
            r = redis.Redis(host='168.32.12.64', port=6379, db=0)
        except:
            pass
        local_base_dir = './realtime_data'
        timestamp = date.today()
    else:
        print('使用开发模式运行')
        mysql_connect = MysqlConnect(host='168.32.63.69', port=3201, user='root', passwd='root')
        ftp_connect = FtpConnect(host='168.32.63.60', directory='./test')
        try:
            r = redis.Redis(host='168.32.63.67', port=6379, db=0)
        except:
            pass
        local_base_dir = 'd:/realtime_data'
        timestamp = date.today() - datetime.timedelta(days=0)
    pattern = ''
    # 删除3天前的数据
    past_time = timestamp - datetime.timedelta(days=3)
    # 当前时间戳
    t = time.localtime()
    try:

        # 获得目标数据库的连接句柄
        conn = initial_conf(mysql_connect)
        # try:
        #     r = redis.Redis(host='168.32.12.64', port=6379, db=0)
        # except:
        #     pass

        # 从总行ftp上拽当天数据
        get_ftp_files(host=ftp_connect.host, user=ftp_connect.user, passwd=ftp_connect.passwd\
                      , remote_dir=ftp_connect.directory, local_base_dir=local_base_dir, pattern=pattern\
                      , timestamp=timestamp, conn=conn)
        # 解压缩当天数据
        decompress_Z_file(local_dir=local_base_dir, timestamp=timestamp, conn=conn)
        # 将数据写入DB
        ins_data_to_appdb(local_base_dir=local_base_dir, timestamp=timestamp, conn=conn, r=r)
        # 1点清理数据
        if t.tm_hour == 1:
            del_past_data(local_base_dir=local_base_dir,timestamp=past_time, conn=conn)
    finally:
        conn.close()


# 入口点
logger = ini_logger()
main()
#schedule.every(20).minutes.do(main)

#while True:
#    schedule.run_pending()




