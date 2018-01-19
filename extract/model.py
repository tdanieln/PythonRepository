# -*- coding: utf-8 -*-
import log

__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class Column(object):
    """
    # 列信息
    """
    def __init__(self, order, column_name, column_type, column_length=0, start_position=0\
                 , end_position=0, decimal_length=0, decimal_point_length=0):
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

    def __eq__(self, other):
        ret_value = False

        if self.order == other.order and self.column_name == other.column_name \
            and self.column_length == other.column_length \
            and self.start_position == other.start_position \
            and self.end_position == other.end_position :
            ret_value = True
        return ret_value


class Model(object):
    # 模型
    """

    """
    pass


class EDIPModel(Model):
    # EDIP数据模型
    """

    """
    def __init__(self, model_name, row_length, column_count, list_column, row_count, table_name='', file_size=0):
        self.model_name = model_name
        if len(table_name) == 0:
            self.table_name = model_name
        else:
            self.table_name = table_name
        self.model_name = model_name
        self.row_count = row_count
        self.row_length = row_length
        self.column_count = column_count
        self.list_column = list_column
        self.file_size = file_size

    def generate_create_table_sql(self, db_manager):
        if self.list_column is None or len(self.list_column) == 0:
            logger.error('生成对象为Null或者列属性为空')
            raise Exception('生成对象为Null或者列属性为空')

        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')

        create_table_sql = db_manager.gen_create_table_sql(table_name=self.table_name, list_column=self.list_column)
        return create_table_sql

    def generate_insert_sql(self, db_manager):
        if self.list_column is None or len(self.list_column) == 0:
            logger.error('生成对象为Null或者列属性为空')
            raise Exception('生成对象为Null或者列属性为空')

        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')

        insert_sql = db_manager.gen_insert_sql(table_name=self.table_name, list_column=self.list_column)
        return insert_sql

    def generate_update_sql(self, db_manager):
        if self.list_column is None or len(self.list_column) == 0:
            logger.error('生成对象为Null或者列属性为空')
            raise Exception('生成对象为Null或者列属性为空')

        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')

        update_sql = db_manager.gen_update_sql(table_name=self.table_name, list_column=self.list_column)
        return update_sql

    def generate_truncate_sql(self, db_manager):
        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')

        truncate_sql = db_manager.gen_truncate_sql(table_name=self.table_name)
        return truncate_sql

    def generate_get_table_info_sql(self, db_manager):
        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')
        get_table_info_sql = db_manager.gen_get_table_info_sql()
        return get_table_info_sql

    def generate_get_table_column_info_sql(self, db_manager):
        if self.table_name is None or len(self.table_name) == 0:
            logger.error('生成对象为Null或者表名为空')
            raise Exception('生成对象为Null或者表名为空')
        get_table_column_info_sql = db_manager.gen_get_table_column_info_sql()
        return get_table_column_info_sql


    def __eq__(self, other):
        # 应该有为None的判断
        ret_value = True
        if self.model_name != other.model_name:
            ret_value = False
        elif self.row_length != other.row_length:
            ret_value = False
        elif self.column_count != other.column_count:
            ret_value = False

        if ret_value is True:
            if len(self.list_column) != len(other.list_column):
                ret_value = False
            else:
                for i in range(0, len(self.list_column)):
                    if self.list_column[i] != other.list_column[i]:
                        ret_value = False
                        break
        return ret_value









