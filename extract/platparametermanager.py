# -*- coding: utf-8 -*-
import log
import os
__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class Parameter(object):
    def __init__(self, source_parameter=None, db_parameter=None, model_parameter=None, strategy_parameter=None):
        self.source_parameter = source_parameter
        self.db_parameter = db_parameter
        self.model_parameter = model_parameter
        self.strategy_parameter = strategy_parameter


class PlatformParameter(object):
    pass


class PlatformParameterFromDB(PlatformParameter):
    pass


class PlatformParameterFromDBForBJ(PlatformParameterFromDB):
    def __init__(self, database_name='', source_config_table_name='', db_config_table_name=''
                 , model_config_table_name='', strategy_config_table_name='', platform_manager_id=0):
        self.database_name = database_name
        self.database_name = self.database_name.strip()
        self.source_config_table_name = source_config_table_name
        self.db_config_table_name = db_config_table_name
        self.model_config_table_name = model_config_table_name
        self.strategy_config_table_name = strategy_config_table_name
        self.platform_manager_id = platform_manager_id

    def get_database_name_and_table_name(self, table_name):
        return self.database_name + '.' + table_name


class ParameterManager(object):
    def __init__(self, platform_parameter, source_behavior, db_behavior, model_behavior, strategy_behavior=None):
        self.platform_parameter = platform_parameter
        self.source_behavior = source_behavior
        self.db_behavior = db_behavior
        self.model_behavior = model_behavior
        self.strategy_behavior = strategy_behavior

    def add_source_manager_parameter(self, source_parameter, channel):
        self.source_behavior.add_source_manager_config(platform_parameter=self.platform_parameter\
                                                       , source_parameter=source_parameter\
                                                       , channel=channel)

    def get_source_manager_parameter(self, get_source_parameter, channel):
        return self.source_behavior.get_source_manager_config(platform_parameter=self.platform_parameter\
                                                          , source_parameter_query_vo=get_source_parameter\
                                                          , channel=channel)

    def add_db_manager_parameter(self, db_parameter, channel):
        self.db_behavior.add_db_manager_config(platform_parameter=self.platform_parameter\
                                               ,db_manager_parameter=db_parameter, channel=channel)

    def get_db_manager_parameter(self, get_db_parameter_vo, channel):
        return self.db_behavior.get_db_manager_config(platform_parameter=self.platform_parameter\
                                               , db_parameter_query_vo= get_db_parameter_vo
                                               , channel=channel)

    def add_model_manager_parameter(self, model_parameter, channel):
        self.model_behavior.add_model_manager_config(platform_parameter=self.platform_parameter\
                                                     , model_manager_parameter=model_parameter
                                                     , channel=channel)

    def get_model_manager_parameter(self, get_model_parameter_vo, channel):
        return self.model_behavior.get_model_manager_config(platform_parameter=self.platform_parameter\
                                                            , model_manager_query_vo=get_model_parameter_vo
                                                            , channel=channel)

    def add_strategy_manager_parameter(self, strategy_parameter, channel):
        self.strategy_behavior.add_strategy_manager_config(platform_parameter=self.platform_parameter\
                                                     , strategy_parameter=strategy_parameter
                                                     , channel=channel)

    def get_strategy_manager_parameter(self, get_strategy_parameter_vo, channel):
        return self.strategy_behavior.get_strategy_manager_config(platform_parameter=self.platform_parameter\
                                                            , strategy_parameter_query_vo=get_strategy_parameter_vo
                                                            , channel=channel)


class ModelConfig(object):
    pass


class ModelEDIPConfig(ModelConfig):
    def __init__(self, local_dir, buffer_size, model_name, area_no, type_flg, flg_suffix, data_suffix\
                 , model_type, upd_type, table_name, code_set='gbk'):
        self.local_dir = local_dir
        self.model_name = model_name
        self.area_no = area_no
        self.type_flg = type_flg
        self.flg_suffix = flg_suffix
        self.data_suffix = data_suffix
        self.model_name = model_name
        self.model_type = model_type
        self.upd_type = upd_type
        self.table_name = table_name
        self.buffer_size = buffer_size
        self.code_set = code_set

    def get_flg_file_path(self, date_stamp):
        flg_file = os.path.join(self.local_dir, self.model_name)
        flg_file += '.' + date_stamp + '.' + self.area_no + '.' + self.type_flg + '.' + self.flg_suffix
        return flg_file

    def get_data_file_path(self, date_stamp):
        data_file = os.path.join(self.local_dir, self.model_name)
        data_file += '.' + date_stamp + '.' + self.area_no + '.' + self.type_flg + '.' + self.data_suffix
        return data_file


class SourceParameterBehavior(object):
    def get_source_manager_config(self, platform_parameter, source_parameter_query_vo, channel):
        pass

    def add_source_manager_config(self, platform_parameter, source_parameter, channel):
        pass


class SourceParameterBehaviorBJEDIP(SourceParameterBehavior):
    def get_source_manager_config(self, platform_parameter, source_parameter_query_vo, channel):
        source_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.source_config_table_name)
        sql_select_source_config = 'SELECT source_manager_id, source_type, connect_type, ip, port' \
                                   ', user_name, password, pk_path, init_path, database_name' \
                                   ', local_dir, remote_recursion_type, local_recursion_type, remark1, remark2' \
                                   ', remark3, source_charset FROM ' + source_config_table_name + ' WHERE 1=1 '
        sql_para = []
        sql_addition = ''
        if source_parameter_query_vo.source_manager_id != 0 :
            sql_addition += ' AND source_manager_id=%s '
            sql_para.append(source_parameter_query_vo.source_manager_id)
        if source_parameter_query_vo.source_type is not None and len(source_parameter_query_vo.source_type) > 0:
            sql_addition += ' AND source_type like %s'
            sql_para.append('%' + source_parameter_query_vo.source_type + '%')

        sql_select_source_config += sql_addition
        logger.debug('SourceParameterBehaviorBJEDIP get_source_manager_config 执行SQL:' + sql_select_source_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_select_source_config, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            source_parameter = SourceParameterExtractBJ()
            source_parameter.source_manager_id = row[0]
            source_parameter.source_type = row[1]
            source_parameter.connect_type = row[2]
            source_parameter.ip = row[3]
            source_parameter.port = row[4]
            source_parameter.user_name = row[5]
            source_parameter.password = row[6]
            source_parameter.pk_path = row[7]
            source_parameter.init_path = row[8]
            source_parameter.database_name = row[9]
            source_parameter.local_dir = row[10]
            source_parameter.remote_recursion_type = row[11]
            source_parameter.local_recursion_type = row[12]
            source_parameter.remark1 = row[13]
            source_parameter.remark2 = row[14]
            source_parameter.remark3 = row[15]
            source_parameter.source_charset = row[16]
            ret_list.append(source_parameter)
        cursor.close()
        return ret_list

    def add_source_manager_config(self, platform_parameter, source_parameter, channel):
        source_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.source_config_table_name)
        sql_insert_source_config = 'INSERT INTO ' + source_config_table_name + \
                                   '(source_type, connect_type, ip, port, user_name' \
                                   ', password, pk_path, init_path, database_name, local_dir' \
                                   ', remote_recursion_type, local_recursion_type, remark1, remark2, remark3) VALUES ' \
                                   '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        sql_para = [source_parameter.source_type, source_parameter.connect_type, source_parameter.ip,
                    source_parameter.port, source_parameter.user_name, source_parameter.password,
                    source_parameter.pk_path, source_parameter.init_path, source_parameter.database_name,
                    source_parameter.local_dir, source_parameter.remote_recursion_type,
                    source_parameter.local_recursion_type, source_parameter.remark1,
                    source_parameter.remark2, source_parameter.remark3]
        logger.debug('SourceParameterBehaviorBJEDIP add_source_manager_config 执行SQL:' + sql_insert_source_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_insert_source_config, sql_para)
        cursor.close()


class SourceParameter(object):
    pass


class SourceParameterExtractBJ(SourceParameter):
    def __init__(self, source_type='', connect_type='', ip='', port=0, user_name='', password=''\
                 , init_path='', database_name='', pk_path='', remark1='', remark2='', remark3=''
                 , local_dir='', remote_recursion_type=0, local_recursion_type=0
                 , source_manager_id=0):
        self.source_type = source_type
        self.connect_type = connect_type
        self.ip = ip
        self.port = port
        self.user_name = user_name
        self.password = password
        self.init_path = init_path
        self.database_name = database_name
        self.pk_path = pk_path
        self.remark1 = remark1
        self.remark2 = remark2
        self.remark3 = remark3
        self.local_dir = local_dir
        self.remote_recursion_type = remote_recursion_type
        self.local_recursion_type = local_recursion_type
        self.source_manager_id = source_manager_id


class SourceParameterQueryVO(object):
    pass


class SourceParameterBJQueryVO(SourceParameterQueryVO):
    def __init__(self, source_manager_id=0, source_type='', connect_type='', ip='', port=0, user_name='', password=''\
                 , init_path='', database_name='', pk_path='', remark1='', remark2='', remark3='', source_charset='GBK'):
        self.source_manager_id = source_manager_id
        self.source_type = source_type
        self.connect_type = connect_type
        self.ip = ip
        self.port = port
        self.user_name = user_name
        self.password = password
        self.init_path = init_path
        self.database_name = database_name
        self.pk_path = pk_path
        self.remark1 = remark1
        self.remark2 = remark2
        self.remark3 = remark3
        self.source_charset = source_charset


class DBParameterBehavior(object):
    def add_db_manager_config(self, platform_parameter, db_manager_parameter, channel):
        pass

    def get_db_manager_config(self, platform_parameter, db_parameter_query_vo, channel):
        pass


class DBParameterBehaviorBJEDIP(DBParameterBehavior):
    def add_db_manager_config(self, platform_parameter, db_manager_parameter, channel):
        db_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.db_config_table_name)
        sql_insert_db_config = 'INSERT INTO ' + db_config_table_name + '(' \
                               ' db_type, database_name, mysql_engine, mysql_charset, informix_dbs_name' \
                               ', informix_lock_mode, informix_extent_size, informix_next_size) values (' \
                               ' %s,%s,%s,%s,%s,%s,%s,%s)'
        sql_para = [db_manager_parameter.db_type, db_manager_parameter.database_name,
                    db_manager_parameter.mysql_engine,db_manager_parameter.mysql_charset,
                    db_manager_parameter.informix_dbs_name,db_manager_parameter.informix_lock_mode,
                    db_manager_parameter.informix_extent_size,db_manager_parameter.informix_next_size]
        logger.debug('DBParameterBehaviorBJEDIP add_db_manager_config 执行SQL:' + sql_insert_db_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_insert_db_config, sql_para)
        cursor.close()

    def get_db_manager_config(self, platform_parameter, db_parameter_query_vo, channel):
        db_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.db_config_table_name)
        sql_select_db_config = 'SELECT db_manager_id, db_type, database_name, mysql_engine, mysql_charset, ' \
                               'informix_dbs_name, informix_lock_mode, informix_extent_size, informix_next_size, ' \
                               'mysql_buffer_size ' \
                               ' FROM ' + db_config_table_name + ' WHERE 1=1 '
        sql_para = []
        sql_addition = ''
        if db_parameter_query_vo.db_manager_id != 0 :
            sql_addition += ' AND db_manager_id=%s '
            sql_para.append(db_parameter_query_vo.db_manager_id)

        sql_select_db_config += sql_addition
        logger.debug('DBParameterBehaviorBJEDIP get_db_manager_config 执行SQL:' + sql_select_db_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_select_db_config, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            db_parameter = DBParameterBJEdip()
            db_parameter.db_manager_id = row[0]
            db_parameter.db_type = row[1]
            db_parameter.database_name = row[2]
            db_parameter.mysql_engine = row[3]
            db_parameter.mysql_charset = row[4]
            db_parameter.informix_dbs_name = row[5]
            db_parameter.informix_lock_mode = row[6]
            db_parameter.informix_extent_size = row[7]
            db_parameter.informix_next_size = row[8]
            db_parameter.mysql_buffer_size = row[9]
            ret_list.append(db_parameter)
        cursor.close()
        return ret_list


class DBParameter(object):
    pass


class DBParameterBJEdip(DBParameter):
    def __init__(self, db_type='MySQL', database_name='', mysql_engine='', mysql_charset='', informix_dbs_name=''
                 , informix_lock_mode='', informix_extent_size=0, informix_next_size=0, mysql_buffer_size = 1):
        self.db_type = db_type
        self.database_name = database_name
        self.mysql_engine = mysql_engine
        self.mysql_charset = mysql_charset
        self.informix_dbs_name = informix_dbs_name
        self.informix_lock_mode = informix_lock_mode
        self.informix_extent_size = informix_extent_size
        self.informix_next_size = informix_next_size
        self.mysql_buffer_size = mysql_buffer_size


class DBParameterQueryVO(object):
    pass


class DBParameterBJEDIPQueryVO(DBParameterQueryVO):
    def __init__(self, db_manager_id):
        self.db_manager_id = db_manager_id


class ModelParameterBehavior(object):
    def add_model_manager_config(self, platform_parameter, model_manager_parameter, channel):
        pass

    def get_model_manager_config(self, platform_parameter, model_manager_query_vo, channel):
        pass


class ModelParameterBehaviorBJEDIP(ModelParameterBehavior):
    def add_model_manager_config(self, platform_parameter, model_manager_parameter, channel):
        model_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.model_config_table_name)
        sql_insert_model_config = 'INSERT INTO ' + model_config_table_name + '(' \
                                  'database_name, model_info_table_name, column_info_table_name' \
                                  ', model_strategy_table_name) values (%s,%s,%s,%s)'
        sql_para = [model_manager_parameter.database_name, model_manager_parameter.model_info_table_name,
                    model_manager_parameter.column_info_table_name, model_manager_parameter.model_strategy_table_name]
        logger.debug('ModelParameterBehaviorBJEDIP add_model_manager_config 执行SQL:' + sql_insert_model_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_insert_model_config, sql_para)
        cursor.close()

    def get_model_manager_config(self, platform_parameter, model_manager_query_vo, channel):
        model_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.model_config_table_name)
        sql_select_model_config = 'SELECT model_manager_id, database_name, model_info_table_name' \
                                  ', column_info_table_name FROM ' \
                                  + model_config_table_name + ' WHERE 1=1 '
        sql_para = []
        sql_addition = ''
        if model_manager_query_vo.model_manager_id != 0 :
            sql_addition += ' AND model_manager_id=%s '
            sql_para.append(model_manager_query_vo.model_manager_id)
        sql_select_model_config += sql_addition
        logger.debug('ModelParameterBehaviorBJEDIP get_model_manager_config 执行SQL:' + sql_select_model_config)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_select_model_config, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            model_parameter = ModelParameterExtractBJEDIP()
            model_parameter.model_manager_id = row[0]
            model_parameter.database_name = row[1]
            model_parameter.model_info_table_name = row[2]
            model_parameter.column_info_table_name = row[3]
            ret_list.append(model_parameter)
        cursor.close()
        return ret_list


class ModelParameter(object):
    pass


class ModelParameterExtractBJEDIP(ModelParameter):
    def __init__(self, database_name='', model_info_table_name='', column_info_table_name=''
                 , model_strategy_table_name='', model_manager_id=0):
        self.database_name = database_name
        self.model_info_table_name = model_info_table_name
        self.column_info_table_name = column_info_table_name
        self.model_strategy_table_name = model_strategy_table_name
        self.model_manager_id = model_manager_id


class ModelParameterQueryVO(object):
    pass


class ModelExtractBJEDIPParameterQueryVO(ModelParameterQueryVO):
    def __init__(self, model_manager_id):
        self.model_manager_id = model_manager_id


class StrategyParameterBehavior(object):
    def get_strategy_manager_config(self, platform_parameter, source_parameter_query_vo, channel):
        pass

    def add_strategy_manager_config(self, platform_parameter, source_parameter, channel):
        pass


class StrategyParameterBehaviorBJEDIP(StrategyParameterBehavior):
    def get_strategy_manager_config(self, platform_parameter, strategy_parameter_query_vo, channel):
        strategy_config_table_name = platform_parameter.get_database_name_and_table_name(
            platform_parameter.strategy_config_table_name)
        sql_select_strategy_config = 'SELECT strategy_manager_id, strategy_type, database_name, ' \
                                     'model_strategy_table_name, source_log_table_name, source_status_table_name, ' \
                                     'table_log_table_name, table_status_table_name, extract_log_table_name, ' \
                                     'extract_status_table_name, dispatcher_table_name FROM '\
                                     + strategy_config_table_name + ' WHERE 1=1 '

        sql_para = []
        sql_addition = ''
        if strategy_parameter_query_vo.strategy_manager_id != 0 :
            sql_addition += ' AND strategy_manager_id=%s '
            sql_para.append(strategy_parameter_query_vo.strategy_manager_id)
        sql_select_strategy_config += sql_addition
        logger.debug('StrategyParameterBehaviorBJEDIP get_strategy_manager_config 执行SQL:' + sql_select_strategy_config)
        db_conn = channel
        cursor = db_conn.cursor()

        cursor.execute(sql_select_strategy_config, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            strategy_parameter = StrategyParameterBJEDIP()
            strategy_parameter.strategy_manager_id = row[0]
            strategy_parameter.strategy_type = row[1]
            strategy_parameter.database_name = row[2]

            strategy_parameter.model_strategy_table_name = row[3]
            strategy_parameter.source_log_table_name = row[4]
            strategy_parameter.source_status_table_name = row[5]
            strategy_parameter.table_log_table_name = row[6]
            strategy_parameter.table_status_table_name = row[7]
            strategy_parameter.extract_log_table_name = row[8]
            strategy_parameter.extract_status_table_name = row[9]
            strategy_parameter.dispatcher_table_name = row[10]
            ret_list.append(strategy_parameter)
        cursor.close()
        return ret_list

    def add_strategy_manager_config(self, platform_parameter, strategy_parameter, channel):
        pass
        # strategy_config_table_name = platform_parameter.get_database_name_and_table_name(
        #     platform_parameter.strategy_config_table_name)
        # sql_insert_strategy_config = 'INSERT INTO ' + strategy_config_table_name \
        #                              + '(strategy_type, database_name, to_confirm_model_table_name' \
        #                                ', model_strategy_table_name, source_log_table_name, table_log_table_name' \
        #                                ', model_log_table_name, source_status_table_name, table_status_table_name' \
        #                                ', model_status_table_name ' \
        #                                ' VALUES (%s, %s, %s, %s, %s, %s, %s)'
        # sql_para = [strategy_parameter.strategy_type, strategy_parameter.database_name,
        #             strategy_parameter.to_confirm_model_table_name, strategy_parameter.model_strategy_table_name,
        #             strategy_parameter.source_log_table_name, strategy_parameter.table_log_table_name,
        #             strategy_parameter.model_log_table_name, strategy_parameter.source_status_table_name,
        #             strategy_parameter.table_status_table_name, strategy_parameter.model_status_table_name]
        # db_conn = channel
        # cursor = db_conn.cursor()
        # cursor.execute(sql_insert_strategy_config, sql_para)
        # cursor.close()


class StrategyParameter(object):
    pass


class StrategyParameterBJEDIP(StrategyParameter):
    def __init__(self, strategy_type=0, database_name='', to_confirm_model_table_name='',
                 to_confirm_column_table_name='', model_strategy_table_name='',
                 source_log_table_name='', source_status_table_name='', table_log_table_name='',
                 table_status_table_name='', dispatcher_table_name='',extract_log_table_name='',
                 extract_status_table_name='', strategy_manager_id=0):
        self.strategy_type = strategy_type
        self.database_name = database_name
        self.to_confirm_model_table_name = to_confirm_model_table_name
        self.to_confirm_column_table_name = to_confirm_column_table_name
        self.model_strategy_table_name = model_strategy_table_name
        self.source_log_table_name = source_log_table_name
        self.table_log_table_name = table_log_table_name
        self.source_status_table_name = source_status_table_name
        self.table_status_table_name = table_status_table_name
        self.strategy_manager_id = strategy_manager_id
        self.dispatcher_table_name = dispatcher_table_name
        self.extract_log_table_name = extract_log_table_name
        self.extract_status_table_name = extract_status_table_name

    def get_database_with_table(self, table_name):
        if self.database_name is None or len(self.database_name) == 0:
            return table_name
        else:
            return self.database_name + '.' + table_name



