# -*- coding: utf-8 -*-
import datetime

__author__ = 'tdanieln@gmail.com'


class DateUtil:
    # 结息月份元组
    __interested_month_tuple__ = ([3,6,9,12])
    # 结息数据日期
    __interested_flow_data_date__ = 22
    #  结息日期
    __interested_date__ = 21

    @staticmethod
    def get_next_interested_flow_data_date(work_date):
        if isinstance(work_date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(work_date, t)
        elif isinstance(work_date, datetime.datetime):
            dt = work_date
        else:
            return None
        year = dt.year
        month = dt.month
        day = dt.day
        date_next_interest = datetime.datetime(year, month, day)
        next_interested_date = None
        # 如果月份是在结息月份
        if month in DateUtil.__interested_month_tuple__:
            if day < DateUtil.__interested_flow_data_date__:
                next_interested_date = datetime.datetime(year, month, DateUtil.__interested_flow_data_date__)
            else:
                next_interested_date = datetime.datetime(year, month, DateUtil.__interested_flow_data_date__)
                next_interested_date = next_interested_date + datetime.timedelta(days=90)
                next_interested_date = next_interested_date.replace(day = DateUtil.__interested_flow_data_date__)
        # 如果月份不是在结息月份
        else:
            while True:
                date_next_interest = date_next_interest + datetime.timedelta(days=28)
                new_month = date_next_interest.month
                if new_month == month:
                    date_next_interest = date_next_interest + datetime.timedelta(days=4)
                if date_next_interest.month in DateUtil.__interested_month_tuple__:
                    next_interested_date = date_next_interest.replace(day=DateUtil.__interested_flow_data_date__)
                    break

        return next_interested_date

    @staticmethod
    def get_end_of_month(work_date):
        if isinstance(work_date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(work_date, t)
        elif isinstance(work_date, datetime.datetime):
            dt = work_date
        else:
            return None
        year = dt.year
        month = dt.month
        day = dt.day
        date_end_of_month = datetime.datetime(year, month, day)
        date_end_of_month = date_end_of_month + datetime.timedelta(days=28)
        new_month = date_end_of_month.month
        if new_month == month:
            date_end_of_month = date_end_of_month + datetime.timedelta(days=4)
        date_end_of_month = date_end_of_month.replace(day=1)
        date_end_of_month = date_end_of_month - datetime.timedelta(days=1)
        return date_end_of_month

    @staticmethod
    def get_next_end_of_month(work_date):
        if isinstance(work_date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(work_date, t)
        elif isinstance(work_date, datetime.datetime):
            dt = work_date
        else:
            return None
        year = dt.year
        month = dt.month
        day = dt.day
        date_end_of_month = datetime.datetime(year, month, day)
        date_end_of_month = date_end_of_month + datetime.timedelta(days=28)
        new_month = date_end_of_month.month
        if new_month == month:
            date_end_of_month = date_end_of_month + datetime.timedelta(days=4)
        # 次月1号
        date_end_of_month = date_end_of_month.replace(day=1)
        # 次次月
        date_end_of_month = date_end_of_month + datetime.timedelta(days=32)
        date_end_of_month = date_end_of_month.replace(day=1)
        date_end_of_month = date_end_of_month - datetime.timedelta(days=1)
        return date_end_of_month

    @staticmethod
    def get_next_date(date):
        if isinstance(date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(date, t)
        elif isinstance(date, datetime.datetime):
            dt = date
        else:
            return None
        dt = dt + datetime.timedelta(days=1)
        return dt

    @staticmethod
    def is_end_of_month(work_date):
        if isinstance(work_date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(work_date, t)
        elif isinstance(work_date, datetime.datetime):
            dt = work_date
        else:
            return None
        month = dt.month
        next_date = dt + datetime.timedelta(days=1)
        new_month = next_date.month
        if month != new_month:
            return True
        else:
            return False

    @staticmethod
    def is_interested_date(work_date):
        if isinstance(work_date, datetime.date):
            t = datetime.time(0,0)
            dt = datetime.datetime.combine(work_date, t)
        elif isinstance(work_date, datetime.datetime):
            dt = work_date
        else:
            return None
        month = dt.month
        day = dt.day
        if day == DateUtil.__interested_date__ and month in DateUtil.__interested_month_tuple__:
            return True
        else:
            return False
