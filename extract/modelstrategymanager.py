# -*- coding: utf-8 -*-
import log
import const
from util import DateUtil

__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class ModelStrategy(object):
    pass


class ModelStrategyBJEDIP(ModelStrategy):
    def __init__(self, model_name=None, data_date=None, model_strategy_id=0, area_no='320000', type_flg='0000',
                 flg_suffix='flg', data_suffix='dat.gz', model_type=1, upd_type=0, table_name=None, table_name_type=0,
                 model_stamp_field_name=None, model_stamp_type=None, primary_key=None,   snap_type=0
                 , frequency_type=1, transform_type=0, next_work_date=None, block_code=None, order_level=99,
                 verify_type=1, status=1):
        self.model_name = model_name
        if table_name is None:
            self.table_name = model_name
        else:
            self.table_name = table_name
        self.model_strategy_id = model_strategy_id
        self.data_date = data_date
        self.area_no = area_no
        self.type_flg = type_flg
        self.flg_suffix = flg_suffix
        self.data_suffix = data_suffix
        self.model_type = model_type
        self.upd_type = upd_type
        self.snap_type = snap_type
        self.status = status
        self.model_stamp_field_name = model_stamp_field_name
        self.model_stamp_type = model_stamp_type
        self.primary_key = primary_key
        self.transform_type = transform_type
        self.table_name_type = table_name_type
        self.frequency_type = frequency_type
        self.next_work_date = next_work_date
        self.block_code = block_code
        self.order_level = order_level
        self.verify_type = verify_type


class ModelStrategyManager(object):
    def __init__(self
                 , platform_parameter
                 , register_model_strategy_behavior
                 , get_model_strategy_behavior
                 , update_model_strategy_behavior
                 , check_model_strategy_behavior):
        self.platform_parameter = platform_parameter
        self.register_model_strategy_behavior = register_model_strategy_behavior
        self.get_model_strategy_behavior = get_model_strategy_behavior
        self.update_model_strategy_behavior = update_model_strategy_behavior
        self.check_model_strategy_behavior = check_model_strategy_behavior

    def register_model_strategy(self, model_strategy, channel):
        self.register_model_strategy_behavior.register_model_strategy(platform_parameter=self.platform_parameter
                                                                    , model_strategy=model_strategy
                                                                    , channel=channel)

    def get_model_strategy(self, model_strategy, channel):
        return self.get_model_strategy_behavior.get_model_strategy(platform_parameter=self.platform_parameter
                                                            , model_strategy=model_strategy
                                                            , channel=channel)

    def update_model_strategy(self, model_strategy, channel):
        return self.update_model_strategy_behavior.update_model_strategy(platform_parameter=self.platform_parameter
                                                                    , model_strategy=model_strategy
                                                                    , channel=channel)

    def check_model_strategy(self, model_strategy, work_date, db_manager=None, channel=None):
        return self.check_model_strategy_behavior.is_next_work_date(model_strategy=model_strategy, work_date=work_date)

    def get_model_next_work_date(self, model_strategy):
        return self.get_model_strategy_behavior.get_model_next_work_date(model_strategy=model_strategy)


class RegisterModelStrategyBehavior(object):
    def register_model_strategy(self, platform_parameter, model_strategy, channel):
        pass


class RegisterModelStrategyBehaviorBJEDIP(RegisterModelStrategyBehavior):
    def register_model_strategy(self, platform_parameter, model_strategy, channel):
        model_strategy_table_name = platform_parameter.strategy_parameter.get_database_with_table(
            platform_parameter.strategy_parameter.model_strategy_table_name)
        sql_insert_model_strategy = 'INSERT INTO ' + model_strategy_table_name + '(' +\
                                    'model_name, area_no, type_flg, flg_suffix, data_suffix' \
                                    ', model_type, upd_type, snap_type,  table_name, data_date' \
                                    ', next_work_date, frequency_type, block_code, order_level, verify_type,' \
                                    ' status)' \
                                    ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        sql_para = [model_strategy.model_name, model_strategy.area_no,
                    model_strategy.type_flg, model_strategy.flg_suffix,
                    model_strategy.data_suffix, model_strategy.model_type,
                    model_strategy.upd_type, model_strategy.snap_type,
                    model_strategy.table_name, model_strategy.data_date,
                    model_strategy.next_work_date, model_strategy.frequency_type,
                    model_strategy.block_code,model_strategy.order_level,
                    model_strategy.verify_type, model_strategy.status]
        db_conn = channel
        logger.debug('RegisterModelStrategyBehaviorBJEDIP register_model_strategy执行SQL：' + sql_insert_model_strategy)
        cursor = db_conn.cursor()
        cursor.execute(sql_insert_model_strategy, sql_para)
        cursor.close()
        db_conn.commit()


class GetModelStrategyBehavior(object):
    def get_model_strategy(self,platform_parameter, model_strategy, channel):
        pass

    def get_model_next_work_date(self, model_strategy):
        pass


class GetModelStrategyBehaviorBJEDIP(GetModelStrategyBehavior):
    def get_model_strategy(self, platform_parameter, model_strategy, channel):
        # TODODODODODOTOTOTODODODOD 要像其他地方一样生成table名
        model_strategy_table_name = platform_parameter.strategy_parameter.get_database_with_table(
            platform_parameter.strategy_parameter.model_strategy_table_name)
        sql_select_model_strategy = 'SELECT model_strategy_id, model_name, data_date, area_no, type_flg,' \
                                    ' flg_suffix, data_suffix, model_type, upd_type, table_name,' \
                                    ' model_stamp_field_name, model_stamp_type, primary_key, snap_type, transform_type,' \
                                    ' frequency_type, table_name_type, next_work_date, block_code, order_level,' \
                                    ' verify_type, status ' \
                                    ' FROM ' + model_strategy_table_name \
                                    + ' WHERE status=1 '
        sql_para = []
        model_strategy_id = model_strategy.model_strategy_id
        table_name = model_strategy.table_name
        model_name = model_strategy.model_name
        block_code = model_strategy.block_code
        next_work_date = model_strategy.next_work_date
        if model_name is not None and len(model_name) > 0:
            sql_select_model_strategy += ' AND model_name=%s'
            sql_para.append(model_name)
        if model_strategy_id is not None and model_strategy_id > 0:
            sql_select_model_strategy += ' AND model_strategy_id=%s'
            sql_para.append(model_strategy_id)
        if table_name is not None and len(table_name.strip())>0:
            sql_select_model_strategy += ' AND table_name=%s'
            sql_para.append(table_name)
        if next_work_date is not None:
            sql_select_model_strategy += ' AND next_work_date=%s'
            sql_para.append(next_work_date)
        if block_code is not None:
            sql_select_model_strategy += ' AND block_code=%s'
            sql_para.append(block_code)
        sql_select_model_strategy += ' ORDER BY order_level ASC'
        logger.debug('GetModelStrategyBehaviorBJEDIP get_model_strategy执行SQL：' + sql_select_model_strategy)
        db_conn = channel
        cursor = db_conn.cursor()
        cursor.execute(sql_select_model_strategy, sql_para)
        rows = cursor.fetchall()
        ret_list = []
        for row in rows:
            model_strategy_id = row[0]
            model_name = row[1]
            data_date = row[2]
            area_no = row[3]
            type_flg = row[4]

            flg_suffix = row[5]
            data_suffix = row[6]
            model_type = row[7]
            upd_type = row[8]
            table_name = row[9]

            model_stamp_field_name = row[10]
            model_stamp_type = row[11]
            primary_key = row[12]
            snap_type = row[13]
            transform_type = row[14]

            frequency_type = row[15]
            table_name_type = row[16]
            next_work_date = row[17]
            block_code = row[18]
            order_level = row[19]

            verify_type = row[20]
            status = row[21]

            model_strategy = ModelStrategyBJEDIP(model_name=model_name, data_date=data_date,
                                                 model_strategy_id=model_strategy_id, area_no=area_no,
                                                 type_flg=type_flg, flg_suffix=flg_suffix,
                                                 data_suffix=data_suffix, model_type=model_type,
                                                 upd_type=upd_type, snap_type=snap_type,
                                                 status=status, table_name=table_name,
                                                 model_stamp_field_name=model_stamp_field_name,
                                                 model_stamp_type=model_stamp_type,
                                                 primary_key=primary_key, transform_type=transform_type,
                                                 frequency_type=frequency_type, table_name_type=table_name_type,
                                                 block_code=block_code, next_work_date=next_work_date,
                                                 order_level=order_level, verify_type=verify_type)
            ret_list.append(model_strategy)
        cursor.close()
        return ret_list

    def get_model_next_work_date(self, model_strategy):
        data_date = model_strategy.data_date
        next_work_date = None
        frequency_type = model_strategy.frequency_type
        if frequency_type == const.frequency_type_every_day:
            next_work_date = DateUtil.get_next_date(data_date)
        elif frequency_type == const.frequency_type_every_interested:
            next_work_date = DateUtil.get_next_interested_flow_data_date(data_date)
        elif frequency_type == const.frequency_type_every_month:
            next_work_date = DateUtil.get_next_end_of_month(data_date)

        return next_work_date


class UpdateModelStrategyBehavior(object):
    def update_model_strategy(self, platform_parameter, model_strategy, channel):
        pass


class UpdateModelStrategyBehaviorBJEDIP(object):
    def update_model_strategy(self, platform_parameter, model_strategy, channel):
        model_strategy_table_name = platform_parameter.strategy_parameter.get_database_with_table(
            platform_parameter.strategy_parameter.model_strategy_table_name)
        sql_update_model_strategy = 'UPDATE ' + model_strategy_table_name \
                                  + ' SET block_code=%s, data_date=%s ,next_work_date=%s ' \
                                  + ' WHERE model_strategy_id=%s'
        sql_para = [model_strategy.block_code, model_strategy.data_date,
                    model_strategy.next_work_date, model_strategy.model_strategy_id]
        db_conn = channel
        cursor = db_conn.cursor()
        logger.debug('UpdateModelStrategyBehaviorBJEDIP update_model_strategy执行SQL：' + sql_update_model_strategy)
        cursor.execute(sql_update_model_strategy, sql_para)
        cursor.close()


class CheckModelStrategyBehavior(object):
    def is_next_work_date(self, model_strategy, work_date):
        return False

    def is_table_in_db(self, model_strategy, channel):
        return True


class CheckModelStrategyBehaivorBJEDIP(CheckModelStrategyBehavior):
    def is_next_work_date(self, model_strategy, work_date):
        # 其实这里应该是受全局是否严格判定数据日期的变量影响的
        record_data_date = model_strategy.data_date
        next_work_date = model_strategy.next_work_date
        frequency_type = model_strategy.frequency_type
        if next_work_date == work_date:
            return True
        else:
            return False

    def is_table_in_db(self, model_strategy, channel):
        table_name = model_strategy.table_name
        pass

