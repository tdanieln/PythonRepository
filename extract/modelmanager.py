# -*- coding: utf-8 -*-
import model
import re
import datetime
import log

__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class ModelManger(object):
    """
    # 模型管理器
    """
    def __init__(self, generate_model_behavior, register_model_behavior, \
                 get_model_behavior, write_off_model_behavior=None):
        """
        构造方法
        :param generate_model_behavior: 装配创建行为
        :param register_model_behavior: 装配注册行为
        :param get_model_behavior: 装配获取模型行为
        :param write_off_model_behavior:  装配注销模型行为
        :return:
        """
        self.generate_model_behavior = generate_model_behavior
        self.register_model_behavior = register_model_behavior
        self.get_model_behavior = get_model_behavior
        self.write_off_model_behavior = write_off_model_behavior

    def generate_model(self, generate_model_para):
        return self.generate_model_behavior.generate_model(generate_model_para)

    def register_model(self, db_conn, model, start_date, expire_date='99991231'):
        self.register_model_behavior.register_model(db_conn=db_conn, model=model, start_date=start_date, expire_date=expire_date)

    def get_model(self, db_conn, query_model_vo):
        return self.get_model_behavior.get_model(db_conn=db_conn, model_name=query_model_vo.model_name, valid_date=query_model_vo.valid_date)

    def get_models(self, db_conn, query_model_vo):
        return self.get_model_behavior.get_models(db_conn=db_conn, model_name=query_model_vo.model_name, valid_date=query_model_vo.valid_date)

    def write_off_model(self, model):
        pass


class ModelVO(object):
    pass


class GenerateModelBehavior(object):
    """
    # 生成模型行为
    """
    def generate_model(self, generate_model_parameter):
        pass

    def parse_model(self):
        pass


class RegisterModelBehavior(object):
    def __init__(self, register_model_parameter):
        self.register_model_parameter = register_model_parameter

    def register_model(self, model, start_date, expire_date='9999-12-31'):
        pass


class GetModelBehavior(object):
    def __init__(self, get_model_parameter):
        self.get_model_parameter = get_model_parameter

    def get_model(self, model_name, valid_date):
        pass


class WriteOffModelBehavior(object):
    pass

##############################################################################


class GenerateModelParameter():
    pass


class GenerateModelEDIPPara(GenerateModelParameter):
    def __init__(self, model_name, date_stamp, area_no, type_flg, flg_suffix, dat_suffix, local_dir, table_name=''):
        self.model_name = model_name
        self.date_stamp = date_stamp
        self.area_no = area_no
        self.type_flg = type_flg
        self.flg_suffix = flg_suffix
        self.dat_suffix = dat_suffix
        self.local_dir = local_dir
        if len(table_name) == 0:
            self.table_name = model_name
        else:
            self.table_name = table_name


class GenerateModelByEDIPFileParameter(GenerateModelParameter):
    def __init__(self, flg_file_path, is_neglect_inspect=False):
        self.flg_file_path = flg_file_path
        self.is_neglect_inspect = is_neglect_inspect


class GenerateModelByStandardEDIPFileBehavior(GenerateModelBehavior):
    def generate_model(self, generate_model_edip_parameter):
        return GenerateModelByStandardEDIPFileBehavior.parse_flg_file(generate_model_edip_parameter)

    @staticmethod
    def parse_flg_file(generate_model_edip_parameter):
        flg_file_path = generate_model_edip_parameter.flg_file_path
        is_neglect_inspect = generate_model_edip_parameter.is_neglect_inspect
        file_path_pattern = re.compile(r'[\\|/](\w+)\.')
        file_path_match = re.search(file_path_pattern, flg_file_path)
        if file_path_pattern is None:
            raise Exception('')
        default_table_name = file_path_match.group(1)

        logger.debug('开始解析flg文件%s' % flg_file_path)
        row_count_pattern = re.compile(r'^ROWCOUNT')
        row_length_pattern = re.compile(r'^ROWLENGTH')
        file_name_pattern = re.compile(r'^FILENAME')
        file_size_pattern = re.compile(r'^FILESIZE')
        column_count_pattern = re.compile(r'^COLUMNCOUNT')
        column_pattern = re.compile(r'[\d]+\$\$[\w]+\$\$')
        list_column = []
        file_name = None
        with open(flg_file_path, 'r') as f:
            for line in f:
                # 寻找FILENAME的正则
                if file_name_pattern.match(line):
                    file_name = line.split(r'=')[1].strip().split(r'.')[0]
                    if (file_name == default_table_name) is False:
                        logger.error('flg文件%s内指定的数据文件名%s与默认数据文件名%s不一致'\
                                      % (flg_file_path, default_table_name, file_name))
                    continue
                if row_count_pattern.match(line):
                    try:
                        row_count = int(line.split(r'=')[1].strip())
                    except (IndexError, ValueError):
                        # 如果rowcount解析失败，默认赋值为0
                        row_count = 0
                    continue
                if file_size_pattern.match(line):
                    try:
                        file_size = int(line.split(r'=')[1].strip())
                    except (IndexError, ValueError):
                        # 如果file_size解析失败，默认赋值为0
                        file_size = 0
                    continue
                if row_length_pattern.match(line):
                    try:
                        row_length = int(line.split(r'=')[1].strip())
                    except (IndexError, ValueError):
                        # 如果row_length解析失败，默认赋值为0
                        row_length = 0
                    continue
                if column_count_pattern.match(line):
                    try:
                        column_count = int(line.split(r'=')[1].strip())
                    except (IndexError, ValueError):
                        # 如果column_count解析失败，默认赋值为0
                        column_count = 0
                    continue
                if column_pattern.match(line):
                    # 如果是列结构，那么就解析列结构
                    c = GenerateModelByStandardEDIPFileBehavior.analysis_column_content(line)
                    # 把每一列明细内容增加到列结构的list中
                    list_column.append(c)
                    continue
        if is_neglect_inspect is True:
            logger.debug('根据flg文件生成创建文件结构，忽略其他不一致信息')
            file_name = default_table_name
            if list_column is None or len(list_column) == 0:
                logger.error('根据flg文件不能解析出数据结构')
                return None
        else:
            if file_name is None or list_column is None or len(list_column) == 0:
                logger.error('根据flg文件不能解析出数据结构')
                return None

        ret_model = model.EDIPModel(model_name=file_name, row_length=row_length, column_count=column_count,\
                                    list_column=list_column, row_count=row_count, file_size=file_size)
        loginfo = "完成解析flg文件,其对应数据文件名为%s,数据文件行数为%d,单位数据长度为%d,列数应为%d,实际列数为%d" \
                      % (file_name, row_count, row_length, column_count, len(list_column))
        logger.debug(loginfo)
        # 将解析的列结构返回
        return ret_model

    @staticmethod
    def analysis_column_content(line_content):
        #
        """解析flg文件中关于列的内容
        :param line_content:
        :return: 返回column_info对象
        """
        # 先去换行符
        line_trim = line_content.strip()
        content_list = line_trim.split(r'$$')
        position = 0
        column_list = []
        decimal_length = 0
        decimal_point_length = 0
        for content in content_list:
            # 第一个位置是位置序号
            if position == 0:
                column_list.append(int(content))
            # 第二个位置是列名称
            elif position == 1:
                column_list.append(content)
            # 第三个位置是列类型
            elif position == 2:
                content_type_content = content.split(r'(')
                column_list.append(content_type_content[0])
                if content_type_content[0] == 'decimal' or content_type_content[0] == 'DECIMAL' or \
                        content_type_content[0] == 'number' or content_type_content[0] == 'NUMBER' or \
                        content_type_content[0] == 'datetime year to fraction' or content_type_content[0] == 'DATETIME YEAR TO FRACTION':
                    content_type_value = content_type_content[1].strip(r')').split(r',')
                    try:
                        decimal_length = int(content_type_value[0])
                        decimal_point_length = int(content_type_value[1])
                    except IndexError:
                        pass
            # 第四个位置是范围
            elif position == 3:
                content_position = content.split(r',')
                start_position = content_position[0].strip(r'(')
                end_position = content_position[1].strip(r')')
                column_list.append(int(end_position) - int(start_position) + 1)
                column_list.append(int(start_position))
                column_list.append(int(end_position))
            position += 1
        # 如果不是decimal型，则按照普通类型
        if decimal_length is None or decimal_length == 0:
            c = model.Column(column_list[0], column_list[1], column_list[2], column_list[3], column_list[4], column_list[5])
        else:
            c = model.Column(column_list[0], column_list[1], column_list[2], column_list[3], column_list[4],\
                             column_list[5], decimal_length, decimal_point_length)
        return c


class RegisterModelParameter(object):
    pass


class RegisterModelToMySQLParameter(RegisterModelParameter):
    def __init__(self, database, table_model_info, table_column_info):
        self.table_model_info = table_model_info
        self.table_column_info = table_column_info
        self.database = database


class RegisterModelToMySQLBehavior(RegisterModelBehavior):

    def register_model(self, db_conn, model, start_date, expire_date):
        register_model_parameter = self.register_model_parameter
        database = register_model_parameter.database
        database = database.strip()
        table_model_info = register_model_parameter.table_model_info
        table_model_info = table_model_info.strip()
        table_column_info = register_model_parameter.table_column_info
        table_column_info = table_column_info.strip()
        table_name_table_info = database + '.' + table_model_info
        table_name_column_info = database + '.' + table_column_info

        sql_select_table_info = ' SELECT model_id, model_name, row_length, column_count ' \
                                ', start_date, expire_date FROM ' + table_name_table_info

        sql_insert_table_info = ' INSERT INTO ' + table_name_table_info \
                                + '(model_name, row_length, column_count, start_date, expire_date) ' \
                                  'VALUES (%s, %s, %s, %s, %s)'

        sql_insert_column_info = ' INSERT INTO ' + table_name_column_info \
                                 + '(model_id, column_name, column_type, column_order, column_length' \
                                   ', start_position, end_position) values (%s, %s, %s, %s, %s, %s, %s)'

        sql_upd_table_info = 'UPDATE ' + table_name_table_info + ' SET expire_date=%s WHERE model_id=%s'

        sql_select_last_id = 'select LAST_INSERT_ID()'
        conn = db_conn
        # to do 这里应该有判断conn的类型

        cursor = conn.cursor()

        # 1、先查是不是有没有这张表注册的信息
        sql_select_is_exist_table_info = sql_select_table_info + ' WHERE model_name=%s '

        para_select_is_exist = [model.table_name,]
        logger.debug('RegisterModelToMySQLBehavior register_model 执行SQL:' + sql_select_is_exist_table_info)
        cursor.execute(sql_select_is_exist_table_info, para_select_is_exist)
        rows = cursor.fetchall()
        # 如果数据没有注册，那么就注册新的模型数据
        if rows is None or len(rows) == 0:
            # 准备insert table表参数
            para_ins_table_info = self.__prepare_register_table_parameter__(model=model, start_date=start_date)
            # insert table表
            cursor.execute(sql_insert_table_info, para_ins_table_info)
            # 获取table表id
            cursor.execute(sql_select_last_id)
            row = cursor.fetchone()
            table_id = row[0]
            # 准备insert column表参数
            para_ins_column_info = RegisterModelToMySQLBehavior.__prepare_register_column_parameter__(model, table_id)
            # insert column表
            cursor.executemany(sql_insert_column_info, para_ins_column_info)
        else:
            # 如果有注册数据，就要看一下，新增的数据区间是不是在最新的数据区间之后
            sql_select_last_table_info = sql_select_table_info + ' WHERE model_name=%s AND start_date<%s AND' \
                                                                 " expire_date='9999-12-31'"
            para_select_table_info = [model.table_name, start_date]
            logger.debug(sql_select_last_table_info)
            logger.debug(para_select_table_info)
            cursor.execute(sql_select_last_table_info, para_select_table_info)
            row = cursor.fetchone()
            if row is None:
                logger.error('查询到表名为%s的表，但是其最新记录有效期不是9999-12-31' %model.table_name)
                raise Exception('查询到表名为%s的表，但是其最新记录有效期不是9999-12-31' %model.table_name)
            else:
                # 把旧数据表的有效期更新到新记录开始日期的前一天
                old_table_id = row[0]
                date_new_start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
                date_old_expire_date = date_new_start_date - datetime.timedelta(days=1)
                str_old_expire_date = date_old_expire_date.strftime('%Y%m%d')
                para_upd_table_info = [str_old_expire_date, old_table_id]
                # 更新旧数据表数据
                cursor.execute(sql_upd_table_info, para_upd_table_info)
                # 准备insert table表参数
                para_ins_table_info = RegisterModelToMySQLBehavior.__prepare_register_table_parameter__(model, start_date)
                # insert table表
                cursor.execute(sql_insert_table_info, para_ins_table_info)
                # 获取table表id
                cursor.execute(sql_select_last_id)
                row = cursor.fetchone()
                table_id = row[0]
                # 准备insert column表参数
                para_ins_column_info = RegisterModelToMySQLBehavior.__prepare_register_column_parameter__(model, table_id)
                # insert column表
                cursor.executemany(sql_insert_column_info, para_ins_column_info)

        cursor.close()
        conn.commit()

    @staticmethod
    def __prepare_register_table_parameter__(model, start_date, expire_date='9999-12-31'):
        para_list = [model.table_name, model.row_length, model.column_count, start_date, expire_date]
        return para_list

    @staticmethod
    def __prepare_register_column_parameter__(model, table_id):
        para_list = []
        list_column = model.list_column
        for column_info in list_column:
            column_type = 'CHAR'
            is_special_type = False
            # 在解析完字段类型之后，就应该对类型进行规范化处理
            # 先处理CHAR\VARCHAR类型
            if column_info.column_type == 'char' or column_info.column_type == 'CHAR':
                if column_info.column_length >= 255:
                    column_info.column_type = 'VARCHAR'
                else:
                    column_info.column_type = 'CHAR'
            elif column_info.column_type == 'varchar' or column_info.column_type == 'VARCHAR':
                if column_info.column_length <= 60:
                    column_info.column_type = 'CHAR'
                else:
                    column_info.column_type = 'VARCHAR'
            # informix中的lvarchar类型
            elif column_info.column_type == 'lvarchar' or column_info.column_type == 'LVARCHAR':
                if column_info.column_length <= 60:
                    column_info.column_type = 'CHAR'
                else:
                    column_info.column_type = 'VARCHAR'
            elif column_info.column_type == 'nvarchar' or column_info.column_type == 'NVARCHAR':
                if column_info.column_length <= 60:
                    column_info.column_type = 'CHAR'
                else:
                    column_info.column_type = 'VARCHAR'
            # oracle中的varchar2类型
            elif column_info.column_type == 'varchar2' or column_info.column_type == 'VARCHAR2':
                if column_info.column_length <= 60:
                    column_info.column_type = 'CHAR'
                else:
                    column_info.column_type = 'VARCHAR'
            # smallint类型
            elif column_info.column_type == 'smallint' or column_info.column_type == 'SMALLINT':
                column_info.column_type = 'SMALLINT'
            # INT类型的处理
            elif column_info.column_type == 'int' or column_info.column_type == 'INT':
                column_info.column_type = 'INT'
            # LONG类型的处理
            elif column_info.column_type == 'long' or column_info.column_type == 'LONG':
                column_info.column_type = 'LONG'
            # informix专用的serial
            elif column_info.column_type == 'serial8' or column_info.column_type == 'SERIAL8':
                column_info.column_type = 'LONG'
            elif column_info.column_type == 'serial' or column_info.column_type == 'SERIAL':
                column_info.column_type = 'INT'
            # 处理64位专用的int8类型
            elif column_info.column_type == 'int8' or column_info.column_type == 'INT8':
                column_info.column_type = 'LONG'
            # 处理时间类型
            elif column_info.column_type == 'date':
                if column_info.column_length == 8:
                    column_info.column_type = 'DATE'
                else:
                    logger.error('源数据类型为date，但是长度不为8，不能转换为date型')
                    raise Exception('源数据类型为date，但是长度不为8，不能转换为date型')
            # ORACLE的时间戳类型
            elif column_info.column_type == 'DATE':
                if column_info.column_length == 14:
                    column_info.column_type = 'DATETIME'
            # 处理informix专用时间戳类型
            elif column_info.column_type == 'datetime year to fraction':
                if column_info.decimal_point_length == 0:
                    column_info.column_type = 'DATETIME(%d)' %(column_info.decimal_length)
                #column_info.column_type = 'CHAR'
            elif column_info.column_type == 'datetime year to second':
                column_info.column_type = 'DATETIME'
            elif column_info.column_type == 'datetime hour to second':
                column_info.column_type = 'CHAR'
            # DECIMAL类型的处理
            elif column_info.column_type == 'decimal' or column_info.column_type == 'DECIMAL':
                column_info.column_type = 'DECIMAL(%d,%d)' %(column_info.decimal_length,column_info.decimal_point_length)
                is_special_type = True
            # ORACLE特有的Number类型
            elif column_info.column_type == 'number' or column_info.column_type == 'NUMBER':
                is_special_type = True
                # 如果小数点后边为0，那么可以按int或者long来转化
                if column_info.decimal_point_length == 0:
                    # 小于9位则一定是int型，如果不是就位long型
                    if column_info.decimal_length <= 9:
                        column_info.column_type = 'INT'
                    else:
                        column_info.column_type = 'LONG'
                # 如果不是number类型，就转换为decimal类型
                elif column_info.decimal_point_length > 0:
                    column_info.column_type = 'DECIMAL(%d,%d)' % (column_info.decimal_length,column_info.decimal_point_length)
                else:
                    column_info.column_type = 'CHAR'
            elif column_info.column_type == 'boolean':
                column_info.column_type = 'CHAR'
            else:
                logger.error('登记信息时，有未处理的数据类型%s' % column_info.column_type)
                raise Exception('登记信息时，有未处理的数据类型%s' % column_info.column_type)
            if is_special_type:
                single_arr_data = [table_id, column_info.column_name, column_info.column_type \
                                   , column_info.order, column_info.column_length, column_info.start_position\
                                   , column_info.end_position]
            else:
                single_arr_data = [table_id, column_info.column_name, column_info.column_type \
                                   , column_info.order, column_info.column_length, column_info.start_position \
                                   ,column_info.end_position]
            para_list.append(single_arr_data)
        return para_list


class GetModelParameter():
    pass


class GetModelFromDBParameter(GetModelParameter):
    def __init__(self, database, table_model_info, table_column_info):
        self.database = database
        self.table_info = table_model_info
        self.column_info = table_column_info


class QueryModelVO():
    pass


class QueryModelFromDBVO(QueryModelVO):
    def __init__(self, model_name='', valid_date=''):
        self.valid_date = valid_date
        self.model_name = model_name


class GetModelFromDBBehavior(GetModelBehavior):
    def get_model_info(self, db_conn, query_model_from_db_vo):
        get_model_from_db_para = self.get_model_parameter
        conn = db_conn
        database = get_model_from_db_para.database
        database = database.strip()
        model_info = get_model_from_db_para.table_info
        model_info = model_info.strip()
        column_info = get_model_from_db_para.column_info
        column_info = column_info.strip()
        table_name_model_info = database + '.' + model_info
        table_name_column_info = database + '.' + column_info

        sql_select_table_info = ' SELECT model_id, model_name, row_length, column_count ' \
                                ', start_date, expire_date FROM ' + table_name_model_info

        sql_select_column_info = ' SELECT model_id, column_name, column_type, column_order, ' \
                                 ' column_length, start_position, end_position FROM ' + table_name_column_info \
                                 + ' WHERE model_id=%s order by column_order'

        cursor = conn.cursor()
        model_name = query_model_from_db_vo.model_name
        valid_date = query_model_from_db_vo.valid_date

        if model_name is not None and (len(model_name.strip())>0):
            model_name = model_name.strip()

        sql_addition = ' WHERE 1=1 '
        sql_para = []
        if len(model_name)>0:
            sql_addition += ' AND model_name=%s '
            sql_para.append(model_name)

        if valid_date is not None:
            sql_addition += ' AND start_date<=%s AND expire_date>=%s '
            sql_para.append(valid_date)
            sql_para.append(valid_date)

        sql_select_table_info += sql_addition
        logger.debug(sql_select_table_info)
        # 查table_info
        cursor.execute(sql_select_table_info, sql_para)
        rows = cursor.fetchall()

        ret_model_list = []
        for row in rows:
            decimal_length = 0
            table_id = row[0]
            model_name = row[1]
            row_length = int(row[2])
            column_count = int(row[3])
            column_list = []
            cursor.execute(sql_select_column_info, (table_id,))
            col_rows = cursor.fetchall()
            for col_row in col_rows:
                column_name = col_row[1]
                column_type_content = col_row[2].split(r'(')
                column_type = column_type_content[0]
                if column_type == "decimal" or column_type == "DECIMAL":
                    content_type_value = column_type_content[1].strip(r')').split(r',')
                    decimal_length = int(content_type_value[0])
                    decimal_point_length = int(content_type_value[1])
                elif column_type == 'DATETIME' or column_type == 'datetime':
                    try:
                        content_type_value = column_type_content[1].strip(r')')
                        decimal_length = int(content_type_value[0])
                        decimal_point_length = 0
                    except IndexError:
                        pass

                order = col_row[3]
                column_length = col_row[4]
                start_position = col_row[5]
                end_position = col_row[6]
                if decimal_length == 0:
                    c = model.Column(order, column_name, column_type, column_length, start_position, end_position)
                else:
                    c = model.Column(order, column_name, column_type, column_length, start_position, end_position\
                                    , decimal_length, decimal_point_length)
                column_list.append(c)
            ret_model = model.EDIPModel(model_name=model_name, row_length=row_length, column_count=column_count, \
                                        list_column=column_list,row_count=0, table_name=model_name)
            ret_model_list.append(ret_model)
        return ret_model_list

    def get_model(self, db_conn, model_name, valid_date):
        query_model_vo = QueryModelFromDBVO(model_name, valid_date)
        model_list = self.get_model_info(db_conn, query_model_vo)
        if model_list is None or len(model_list) == 0:
            return None
        return model_list[0]

    def get_models(self, db_conn, model_name, valid_date):
        query_model_vo = QueryModelFromDBVO(model_name, valid_date)
        model_list = self.get_model_info(db_conn, query_model_vo)
        return model_list
