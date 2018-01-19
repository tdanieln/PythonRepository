# -*- coding: utf-8 -*-
import log
import sourcemanager
import datetime
import const
__author__ = 'tdanieln@gmail.com'


logger = log.get_logger()


class SourceProcess(object):
    def __init__(self, source_dispatcher, source_manager, platform_parameter):
        self.source_dispatcher = source_dispatcher
        self.source_manager = source_manager
        self.source_parameter = platform_parameter.source_parameter
        self.remote_dir = self.source_parameter.init_path
        self.local_dir = self.source_parameter.local_dir

    def connect(self):
        self.source_manager.connect()

    def source_download_initial(self, model_strategy, work_date, channel):
        pass

    def source_download(self, model_strategy, work_date, channel):
        pass

    def source_complete(self, model_strategy, work_date, channel):
        pass

    def dispose(self):
        self.source_manager.dispose()


class SourceProcessBJEDIP(SourceProcess):
    def __init__(self, source_dispatcher, source_manager, platform_parameter):
        self.source_dispatcher = source_dispatcher
        self.source_manager = source_manager
        self.source_parameter = platform_parameter.source_parameter
        self.remote_dir = self.source_parameter.init_path
        self.local_dir = self.source_parameter.local_dir

    def source_download_initial(self, model_strategy, work_date, channel):
        model_name = model_strategy.model_name
        work_date_str = work_date.strftime('%Y%m%d')
        flg_suffix = model_strategy.flg_suffix
        data_suffix = model_strategy.data_suffix

        status = const.source_status_initial
        list_source_para = []
        upd_list_source_para = []
        # 如果flg配置不为空
        if flg_suffix is not None and len(flg_suffix)>0:
            # 生成flg文件名
            flg_file_name = model_name + '.' + work_date_str
            if model_strategy.area_no is not None and len(model_strategy.area_no) > 0 :
                flg_file_name += '.' +  model_strategy.area_no
            if model_strategy.type_flg is not None and len(model_strategy.type_flg) > 0 :
                flg_file_name += '.' + model_strategy.type_flg

            flg_file_name += '.' + model_strategy.flg_suffix
            # 先查该文件的是否已经有登记情况
            source_list = self.source_dispatcher.get_source_download_status(model_name=model_name
                                                                            , work_date=work_date
                                                                            , file_name=flg_file_name
                                                                            , channel=channel)
            # 只有当数据任务都不存在的时候，才初始化
            if source_list is None or len(source_list) == 0:
                upd_time = datetime.datetime.now()
                # 构造新建flg文件的条件
                flg_source_status_parameter = SourceDispatcherStatusBJEDIP(model_name=model_name
                                                                    , work_date=work_date
                                                                    , file_name=flg_file_name
                                                                    , upd_time=upd_time
                                                                    , status=status)
                list_source_para.append(flg_source_status_parameter)
            else:
                # 如果查到了数据，但是这个数据之前是失败状态的话，则更新为初始状态
                if len(source_list) > 0 :
                    upd_time = datetime.datetime.now()
                    for source in source_list:
                        if source.status == -1:
                            source.status = 0
                            source.upd_time = upd_time
                            upd_list_source_para.append(source)

        # 如果data文件配置不为空
        if data_suffix is not None and len(data_suffix) > 0:
            data_file_name = model_name + '.' + work_date_str
            if model_strategy.area_no is not None and len(model_strategy.area_no) > 0 :
                data_file_name += '.' +  model_strategy.area_no
            if model_strategy.type_flg is not None and len(model_strategy.type_flg) > 0 :
                data_file_name += '.' + model_strategy.type_flg

            data_file_name += '.' + model_strategy.data_suffix
            # data_file_name = model_name + '.' + work_date_str + '.' + model_strategy.area_no + \
            #    '.' + model_strategy.type_flg + '.' + model_strategy.data_suffix
            # 先查该文件的是否已经有登记情况
            source_list = self.source_dispatcher.get_source_download_status(model_name=model_name
                                                                            , work_date=work_date
                                                                            , file_name=data_file_name
                                                                            , channel=channel)
            # 只有当数据任务都不存在的时候，才初始化
            if source_list is None or len(source_list) == 0:
                upd_time = datetime.datetime.now()
                # 构造新建flg文件的条件
                data_source_status_parameter = SourceDispatcherStatusBJEDIP(model_name=model_name
                                                                            , work_date=work_date
                                                                            , file_name=data_file_name
                                                                            , upd_time=upd_time
                                                                            , status=status)
                list_source_para.append(data_source_status_parameter)
            else:
                # 如果查到了数据，但是这个数据之前是失败状态的话，则更新为初始状态
                if len(source_list) > 0 :
                    upd_time = datetime.datetime.now()
                    for source in source_list:
                        if source.status == const.source_status_fail:
                            source.status = const.source_status_initial
                            source.upd_time = upd_time
                            upd_list_source_para.append(source)

        count = self.source_dispatcher.batch_add_source_download_status(list_source_para, channel)
        count_upd = self.source_dispatcher.batch_upd_source_download_status(upd_list_source_para, channel)
        return

    def source_download(self, model_strategy, work_date, channel):
        # 初始化的记录状态
        status = const.source_log_status_initial
        model_name = model_strategy.model_name
        remote_recursion_type = self.source_parameter.remote_recursion_type

        # 先查出指定的模型，指定工作日期的下载任务
        list_source_status = self.source_dispatcher.get_source_download_status(
            status=status, model_name=model_name, work_date=work_date, channel=channel)

        if list_source_status is None or len(list_source_status) == 0:
            return

        down_list = []
        dict ={}
        for source_status in list_source_status:
            file_name = source_status.file_name
            dict[file_name] = source_status
            down_list.append(file_name)
            remote_dir = self.__generate_dir__(remote_recursion_type, work_date, model_name)
        # 构造下载参数
        fetch_parameter = sourcemanager.FetchContentBatchFileParameter(remote_dir=remote_dir
                                                                       , local_dir=self.local_dir
                                                                       , fetch_list=down_list)
        # 构造日志记录参数
        start_time = datetime.datetime.now()
        # 下载数据
        success_list = self.source_manager.fetch(fetch_parameter)
        end_time = datetime.datetime.now()
        cost_time = end_time - start_time
        list_insert_log = []
        list_upd_status = []
        # 成功的处理逻辑
        for success_file in success_list:
            source_log = SourceLogBJEDIP(file_name=success_file,  model_name=model_name, work_date=work_date
                                         , op_type=1)
            source_log.start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            source_log.end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
            source_log.cost_time = cost_time.seconds*1000 + cost_time.microseconds/1000
            source_log.status = const.source_log_status_success
            list_insert_log.append(source_log)
            source_dispatcher_status = dict.get(success_file)
            source_dispatcher_status.upd_time = source_log.end_time
            source_dispatcher_status.status = const.source_status_success
            list_upd_status.append(source_dispatcher_status)
            dict.pop(success_file)

        # 不成功的处理逻辑
        for fail_file_name in dict:
            source_log = SourceLogBJEDIP(file_name=fail_file_name,  model_name=model_name, work_date=work_date
                                         , op_type=1)
            source_log.start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            source_log.status = const.source_log_status_fail
            list_insert_log.append(source_log)
            source_dispatcher_status = dict.get(fail_file_name)
            source_dispatcher_status.status = const.source_status_fail
            source_dispatcher_status.upd_time = source_log.end_time
            list_upd_status.append(source_dispatcher_status)

        count_log = self.source_dispatcher.add_source_download_log(list_insert_log, channel)
        count_status = self.source_dispatcher.batch_upd_source_download_status(list_upd_status, channel)
        # log.debug(count_log)
        # log.debug(count_status)

    def source_complete(self, model_strategy, work_date, channel):
        model_name = model_strategy.model_name
        work_date = work_date
        status = 1
        # 先查出指定的模型，指定工作日期的所有下载任务
        list_all_source_status = self.source_dispatcher.get_source_download_status(
            model_name=model_name, work_date=work_date, channel=channel)
        # 先查出指定的模型，指定工作日期的已经完成的下载任务
        list_source_status = self.source_dispatcher.get_source_download_status(
            status=status, model_name=model_name, work_date=work_date, channel=channel)
        if len(list_all_source_status) == 0:
            ret_value = 0
        elif len(list_all_source_status) == len(list_source_status):
            ret_value = 1
        else:
            ret_value = 2
        return ret_value

    def __generate_dir__(self, recursion_type, work_date, model_name):
        work_date = work_date.strftime('%Y%m%d')
        if recursion_type == const.recursion_type_no:
            rd = self.remote_dir
        elif recursion_type == const.recursion_type_by_year:
            rd = self.remote_dir + '/' + work_date[0:4] + '/'
        elif recursion_type == const.recursion_type_by_year_month:
            rd = self.remote_dir + '/' + work_date[0:6] + '/'
        elif recursion_type == const.recursion_type_by_year_month_day:
            rd = self.remote_dir + '/' + work_date[0:8] + '/'
        elif recursion_type == const.recursion_type_by_file_name:
            rd = self.remote_dir + '/' + model_name + '/'
        return rd


class SourceDispatcherStatus(object):
    pass


class SourceDispatcherStatusBJEDIP(SourceDispatcherStatus):
    def __init__(self, model_name, work_date, file_name='', upd_time=None, status=0, source_status_id=0):
        self.model_name = model_name
        self.work_date = work_date
        self.file_name = file_name
        self.upd_time = upd_time
        self.status = status
        self.source_status_id = source_status_id

    def to_list(self):
        arg_list = [self.model_name, self.work_date, self.file_name, self.upd_time, self.status]
        return arg_list


class SourceLog(object):
    pass


class SourceLogBJEDIP(SourceLog):
    def __init__(self, file_name, model_name='', work_date='',  op_type=0, start_time=None,
                 end_time=None, cost_time=0, status=1):
        self.model_name = model_name
        self.work_date = work_date
        self.file_name = file_name
        self.op_type = op_type
        self.start_time = start_time
        self.end_time = end_time
        self.cost_time = cost_time
        self.status = status

    def to_list(self):
        arg_list = [self.model_name, self.work_date, self.file_name, self.op_type, self.start_time, self.end_time,
                    self.cost_time, self.status]
        return arg_list


class SourceDispatcher(object):
    def __init__(self, platform_parameter):
        pass

    def add_source_download_status(self):
        pass

    def get_source_download_status(self):
        pass

    def upd_source_download_status(self):
        pass

    def add_source_download_log(self):
        pass

    def batch_add_source_download_status(self):
        pass

    def batch_upd_source_download_status(self):
        pass

    def batch_add_source_download_log(self):
        pass


class SourceDispatcherBJEDIP(SourceDispatcher):
    def __init__(self, platform_parameter):
        strategy_parameter = platform_parameter.strategy_parameter
        self.source_status_table_name = strategy_parameter.get_database_with_table(strategy_parameter.source_status_table_name)
        self.source_log_table_name = strategy_parameter.get_database_with_table(strategy_parameter.source_log_table_name)

    def add_source_download_status(self, status_source_parameter, channel):
        sql = 'INSERT INTO ' + self.source_status_table_name \
              + ' (model_name, work_date, file_name, upd_time,status) VALUES (%s,%s,%s,%s,%s)'
        sql_args = status_source_parameter.to_list()
        logger.debug('SourceDispatcherBJEDIP add_source_download_status 执行SQL：' + sql)
        cursor = channel.cursor()
        count = cursor.execute(sql, sql_args)
        cursor.close()
        return count

    def batch_add_source_download_status(self, list_status_source_parameter, channel):
        sql = 'INSERT INTO ' + self.source_status_table_name \
              + ' (model_name, work_date, file_name, upd_time,status) VALUES (%s,%s,%s,%s,%s)'
        sql_args = []
        for status_source_parameter in  list_status_source_parameter:
            sql_args.append(status_source_parameter.to_list())
        logger.debug('SourceDispatcherBJEDIP batch_add_source_download_status 执行SQL：' + sql)
        cursor = channel.cursor()
        count = cursor.executemany(sql, sql_args)
        cursor.close()
        return count

    def get_source_download_status(self, model_name='', status=None, work_date=None, file_name='', channel=None):
        sql_get_source_status = 'SELECT source_status_id, model_name, work_date, file_name, status FROM ' \
                                + self.source_status_table_name + ' WHERE 1=1 '
        sql_para = []
        sql_addition = ''
        if model_name is not None and len(model_name) > 0:
            sql_addition += ' AND model_name=%s'
            sql_para.append(model_name)
        if work_date is not None :
            sql_addition += ' AND work_date=%s'
            sql_para.append(work_date)
        if file_name is not None and len(file_name) > 0:
            sql_addition += ' AND file_name=%s'
            sql_para.append(file_name)
        if status is not None :
            sql_addition += ' AND status=%s'
            sql_para.append(status)
        sql_get_source_status += sql_addition
        logger.debug('SourceDispatcherBJEDIP get_source_download_status 执行SQL：' + sql_get_source_status)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_get_source_status, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            source_status_id = row[0]
            model_name = row[1]
            work_date = row[2]
            file_name = row[3]
            status = row[4]
            status_ctrl_para = SourceDispatcherStatusBJEDIP(model_name=model_name, work_date=work_date
                                                                ,file_name=file_name, status=status
                                                                ,source_status_id=source_status_id)
            ret_list.append(status_ctrl_para)
        cursor.close()
        return ret_list

    def upd_source_download_status(self, status_source_parameter, channel):
        sql_upd_source_download_status = 'UPDATE ' + self.source_status_table_name + ' SET status=%s ' \
                                                                                     ', upd_time=%s ' \
                                                                                     'WHERE source_status_id=%s'
        source_status_id = status_source_parameter.source_status_id
        status = status_source_parameter.status
        upd_time = status_source_parameter.upd_time
        args = [status, upd_time,  source_status_id]
        logger.debug('SourceDispatcherBJEDIP upd_source_download_status 执行SQL：' + sql_upd_source_download_status)
        db_conn = channel
        cursor = db_conn.cursor()
        count = cursor.execute(sql_upd_source_download_status, args)
        return count

    def batch_upd_source_download_status(self, list_status_source_parameter, channel):
        sql_upd_source_download_status = 'UPDATE ' + self.source_status_table_name + ' SET status=%s' \
                                                                                     ', upd_time=%s ' \
                                                                                     'WHERE source_status_id=%s'
        sql_args_list = []
        for status_source_parameter in list_status_source_parameter:
            source_status_id = status_source_parameter.source_status_id
            upd_time = status_source_parameter.upd_time
            status = status_source_parameter.status
            args = [status, upd_time, source_status_id]
            sql_args_list.append(args)
        logger.debug('SourceDispatcherBJEDIP batch_upd_source_download_status 执行SQL：' + sql_upd_source_download_status)
        db_conn = channel
        cursor = db_conn.cursor()
        count = cursor.executemany(sql_upd_source_download_status, sql_args_list)
        return count

    def add_source_download_log(self, list_log_source_parameter, channel):
        sql = 'INSERT INTO ' + self.source_log_table_name \
              + ' (model_name, work_date, file_name, op_type, start_time' \
                ', end_time, cost_time, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
        sql_args = []
        for log_source_parameter in  list_log_source_parameter:
            sql_args.append(log_source_parameter.to_list())
        logger.debug('SourceDispatcherBJEDIP add_source_download_log 执行SQL：' + sql)
        cursor = channel.cursor()
        count = cursor.executemany(sql, sql_args)
        cursor.close()
        return count

