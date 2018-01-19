# -*- coding: utf-8 -*-
import platparametermanager
import log

__author__ = 'tdanieln@gmail.com'


logger = log.get_logger()


class PlatformManager(object):
    def __init__(self, register_platform_para_behavior=None, get_platform_para_behavior=None):
        self.register_platform_para_behavior = register_platform_para_behavior
        self.get_platform_para_behavior = get_platform_para_behavior

    def register_platform_parameter(self, platform_parameter, channel):
        if self.register_platform_para_behavior is None:
            return
        self.register_platform_para_behavior.register_platform_parameter(platform_parameter=platform_parameter, channel=channel)

    def get_platform_parameter(self, platform_parameter, channel):
        return self.get_platform_para_behavior.get_platform_parameter(platform_parameter=platform_parameter, channel=channel)


class RegisterPlatformParameterBehavior(object):
    def register_platform_parameter(self, platform_parameter, channel):
        pass


class RegisterPlatformParameterBehaviorBJEDIP(RegisterPlatformParameterBehavior):
    def register_platform_parameter(self, platform_parameter, channel):
        table_name = platform_parameter.database_name + '.aaa_platform_manager_config'
        sql_insert_platform = 'INSERT INTO ' + table_name \
                              + '(database_name, source_config_table_name, db_config_table_name' \
                                ', model_config_table_name) VALUES (%s,%s,%s,%s)'
        sql_para = [platform_parameter.database_name, platform_parameter.source_config_table_name,
                    platform_parameter.db_config_table_name, platform_parameter.model_config_table_name]
        db_conn = channel
        cursor = db_conn.cursor()
        logger.debug('RegisterPlatformParameterBehaviorBJEDIP 执行SQL:' + sql_insert_platform)
        cursor.execute(sql_insert_platform, sql_para)
        cursor.close()


class GetPlatformParameterBehavior(object):
    def get_platform_parameter(self, platform_parameter, channel):
        pass


class GetPlatformParameterBehaviorBJEDIP(GetPlatformParameterBehavior):
    def get_platform_parameter(self, platform_parameter, channel):
        table_name = 'aaa_platform_manager_config'
        sql_select_platform = 'SELECT platform_manager_id, database_name, source_config_table_name, db_config_table_name' \
                              ', model_config_table_name, strategy_config_table_name FROM ' + table_name \
                              + ' WHERE 1=1 '
        sql_para = []
        sql_addition = ''
        if platform_parameter is not None:
            if platform_parameter.platform_manager_id == 0 is False:
                sql_addition += ' AND platform_manager_id=%s '
                sql_para.append(platform_parameter.platform_id)
            sql_select_platform += sql_addition
        logger.debug('GetPlatformParameterBehaviorBJEDIP 执行SQL:' + sql_select_platform)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_select_platform, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            plat_parameter = platparametermanager.PlatformParameterFromDBForBJ()
            plat_parameter.platform_id = row[0]
            plat_parameter.database_name = row[1]
            plat_parameter.source_config_table_name = row[2]
            plat_parameter.db_config_table_name = row[3]
            plat_parameter.model_config_table_name = row[4]
            plat_parameter.strategy_config_table_name = row[5]
            ret_list.append(plat_parameter)
        cursor.close()
        return ret_list
