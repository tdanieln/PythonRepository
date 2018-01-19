# -*- coding: utf-8 -*-
import util
import const
import log
__author__ = 'tdanieln@gmail.com'

logger = log.get_logger()


class ReviewProcess(object):
    def __init__(self, db_manager, platform_parameter):
        self.db_manager = db_manager
        self.platform_parameter = platform_parameter

    @staticmethod
    def review_result(model_strategy):
        work_date = model_strategy.work_date
        next_work_date = model_strategy.next_work_date
        snap_type = model_strategy.snap_type

    @staticmethod
    def check_snap(model_strategy):
        snap_type = model_strategy.snap_type
        if snap_type is None or snap_type == const.snap_type_no_snap:
            return False
        else:
            return True

    @staticmethod
    def check_transform(model_strategy):
        transform_type = model_strategy.transform_type
        frequency_type = model_strategy.frequency_type
        work_date = model_strategy.data_date
        if transform_type is None or transform_type == 0 :
            return False
        elif transform_type == 1:
            if work_date.month == 12:
                compute_datetime = util.DateUtil.get_end_of_month(work_date)
                if frequency_type == const.frequency_type_every_day:
                    if work_date == compute_datetime.date():
                        return True
                    else:
                        return False
                elif frequency_type == const.frequency_type_every_interested:
                    return True
            else:
                return False

    @staticmethod
    def is_snap_date(work_date, model_strategy):
        snap_type = model_strategy.snap_type
        if snap_type == const.snap_type_every_day:
            return True
        elif snap_type == const.snap_type_month_end:
            if util.DateUtil.is_end_of_month(work_date):
                return True
            else:
                return False
        elif snap_type == const.snap_type_interest:
            if util.DateUtil.is_interested_date(work_date):
                return True
            else:
                return False
        elif snap_type == const.snap_type_month_end_interest:
            if util.DateUtil.is_interested_date(work_date):
                return True
            else:
                if util.DateUtil.is_end_of_month(work_date):
                    return True
                else:
                    return False
        else:
            return False

    def create_snap(self, work_date, model_strategy, channel):
        old_table_name = model_strategy.table_name
        work_date_str = work_date.strftime('%Y%m%d')
        new_table_name = 'zzz_snap_' + old_table_name + '_' + work_date_str
        sql = self.db_manager.gen_save_to_other_table_sql(old_table_name, new_table_name)
        cursor = channel.cursor()
        logger.debug('ReviewProcess create_snap 执行SQL:' + sql)
        cursor.execute(sql)
        cursor.close()
