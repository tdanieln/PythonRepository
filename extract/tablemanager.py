# -*- coding: utf-8 -*-
import model
import log

__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class TableManager(object):
    def __init__(self, create_behavior=None, check_behavior=None, alter_behavior=None, drop_behavior=None):
        self.create_behavior = create_behavior
        self.alter_behavior = alter_behavior
        self.drop_behavior = drop_behavior
        self.check_behavior = check_behavior

    def create_table(self, model, channel):
        self.create_behavior.create_table(model=model, channel=channel)

    def alter_table(self):
        pass

    def drop_table(self):
        pass

    def check_table_exist(self, model, channel):
        return self.check_behavior.check_exist(model=model, channel=channel)

    def check_table_artchitecture(self, model, channel):
        return self.check_behavior.check_table_architecture(check_model=model, channel=channel)


class CreateTableBehavior(object):
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def create_table(self, model, channel):
        cursor = channel.cursor()
        db_manager = self.db_manager
        sql = model.generate_get_table_info_sql(db_manager)
        sql_para = [model.table_name]
        count = cursor.execute(sql, sql_para)
        print(count)
        if count == 0:
            sql_create_table = model.generate_create_table_sql(db_manager)
            print(sql_create_table)
            cursor.execute(sql_create_table)
        cursor.close()


class AlterTableBehavior(object):
    def alter_table(self, db_conn, db_manager, model):
        pass


class DropTableBehavior(object):
    def drop_table(self, db_conn, db_manager, model):
        pass


class CheckTableBehavior(object):
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def check_exist(self, model, channel):
        cursor = channel.cursor()
        db_manager = self.db_manager
        sql = model.generate_get_table_info_sql(db_manager)
        sql_para = [model.table_name]
        cursor.execute(sql, sql_para)
        count = cursor.fetchone()

        if count is None or count[0] == 0:
            return False
        else:
            return True

    def check_table_architecture(self,check_model, channel):
        cursor = channel.cursor()
        db_manager = self.db_manager
        sql = check_model.generate_get_table_column_info_sql(db_manager)
        sql_para = [check_model.table_name]
        cursor.execute(sql, sql_para)
        rows = cursor.fetchall()
        db_column_list = []
        if rows is None:
            pass
        else:
            for row in rows:
                name = row[0]
                order = row[1]
                type = row[2]
                c = model.Column(order=order, column_name=name, column_type=type)
                db_column_list.append(c)
        column_list = check_model.list_column
        if len(db_column_list) == 0:
            ret_value = False
        elif len(db_column_list) != len(column_list):
            ret_value = False
        else:
            ret_value = True
            for i in range(0, len(db_column_list)):
                if column_list[i].order != db_column_list[i].order or \
                                column_list[i].column_name != db_column_list[i].column_name:
                    ret_value = False
                    break
        return ret_value


class CreateTableParameter(object):
    def __init__(self, db_manager):
        self.db_manager = db_manager


