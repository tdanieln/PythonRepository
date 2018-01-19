# -*- coding: utf-8 -*-
import modelmanager
import modelstrategymanager
import datetime
import const

__author__ = 'tdanieln@gmail.com'


class InspectProcess(object):
    def __init__(self, model_manager, model_strategy_manager, table_manager, platform_parameter):
        self.model_manager = model_manager
        self.source_parameter = platform_parameter.source_parameter
        self.local_dir = self.source_parameter.local_dir
        self.model_strategy_manager = model_strategy_manager
        self.table_manager = table_manager

    def inspect_model_registered(self, model_strategy, work_date, channel):
        pass

    def inspect_table_exist(self, model_strategy, work_date, channel):
        pass

    def inspect_model_table_consistency(self, model_strategy, work_date, channel, parse_model=None):
        pass

    def inspect_model_data_date(self, model_strategy, work_date, channel):
        pass


class InspectProcessBJEDIP(InspectProcess):
    def __init__(self, model_manager, model_strategy_manager, table_manager, platform_parameter):
        self.model_manager = model_manager
        self.source_parameter = platform_parameter.source_parameter
        self.local_dir = self.source_parameter.local_dir
        self.model_strategy_manager = model_strategy_manager
        self.table_manager = table_manager

    def inspect_model_registered(self, model_strategy, work_date, channel):
        model_name = model_strategy.model_name
        verify_type = model_strategy.verify_type
        # 默认需要进行数据验证
        need_verify = True
        if verify_type is None:
            need_verify = True
        else:
            if verify_type == const.verify_type_no:
                need_verify = False

        work_date_str = work_date.strftime('%Y%m%d')
        flg_file_name = model_name + '.' + work_date_str
        # flg_file_name = model_name + '.' + work_date_str + '.' + model_strategy.area_no + '.' \
        #                 + model_strategy.type_flg + '.' + model_strategy.flg_suffix
        if model_strategy.area_no is not None and len(model_strategy.area_no) > 0 :
            flg_file_name += '.' +  model_strategy.area_no
        if model_strategy.type_flg is not None and len(model_strategy.type_flg) > 0 :
            flg_file_name += '.' + model_strategy.type_flg
        flg_file_name += '.' + model_strategy.flg_suffix

        if need_verify is True:
            flg_file_path = self.local_dir + '/' + flg_file_name
            general_model_parameter = modelmanager.GenerateModelByEDIPFileParameter(flg_file_path)
            parse_model = self.model_manager.generate_model(general_model_parameter)
            query_model_vo = modelmanager.QueryModelFromDBVO(model_name=model_name, valid_date=work_date_str)
            registered_model = self.model_manager.get_model(query_model_vo=query_model_vo, db_conn=channel)

            if parse_model == registered_model:
                return True
            else:
                # raise Exception('模型:' + model_name + '，模型日期：' + work_date + '与登记不一致')
                return False
        else:
            return True

    def inspect_table_exist(self, model_strategy, work_date, channel, model=None):
        model_name = model_strategy.model_name
        work_date_str = work_date.strftime('%Y%m%d')
        query_model_vo = modelmanager.QueryModelFromDBVO(model_name=model_name, valid_date=work_date_str)
        registered_model = self.model_manager.get_model(query_model_vo=query_model_vo, db_conn=channel)
        registered_model.table_name = model_strategy.table_name
        return self.table_manager.check_table_exist(model=registered_model, channel=channel)

    def inspect_model_table_consistency(self, model_strategy, work_date, channel, model=None):
        model_name = model_strategy.model_name
        work_date_str = work_date.strftime('%Y%m%d')
        query_model_vo = modelmanager.QueryModelFromDBVO(model_name=model_name, valid_date=work_date_str)
        registered_model = self.model_manager.get_model(query_model_vo=query_model_vo, db_conn=channel)
        registered_model.table_name = model_strategy.table_name
        return self.table_manager.check_table_artchitecture(model=registered_model, channel=channel)

    def inspect_model_data_date(self, model_strategy, work_date, channel):
        model_strategy_query_vo = modelstrategymanager.ModelStrategyBJEDIP(model_name=model_strategy.model_name,
                                                                           table_name=model_strategy.table_name)
        model_strategy_list = self.model_strategy_manager.get_model_strategy(model_strategy=model_strategy_query_vo
                                                                             , channel=channel)
        if len(model_strategy_list) != 1:
            return False
        record_data_date = model_strategy_list[0].data_date
        frequency_type = model_strategy_list[0].frequency_type
        # record_data_date = datetime.datetime.strptime(record_data_date_str, '%Y-%m-%d')
        date_work_date = work_date
        # 如果下发策略是每日下发
        if frequency_type == 1:
            if (date_work_date-record_data_date).days != 1:
                return False
            else:
                return True
        return True
