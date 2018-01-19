# -*- coding: utf-8 -*-
import log

__author__ = 'user'

logger = log.get_logger()


class InitialProcess(object):
    def __init__(self, model_manager, model_strategy_manager, dispatcher_manager):
        self.model_manager= model_manager
        self.dispatcher_manager = dispatcher_manager
        self.model_strategy_manager = model_strategy_manager

    def get_work_models(self, query_model_vo, channel):
        pass

    def get_model_strategy(self, query_strategy_vo, channel):
        pass

    def init_dispatcher(self, dispatcher_ctrl_parameter, model_strategy, channel):
        pass


class InitialProcessBJEDIP(InitialProcess):
    def get_work_models(self, query_model_vo, channel):
        """
        第一步，找到符合当前条件的模型列表
        :param query_model_vo:
        :param channel:
        :return:
        """
        return self.model_manager.get_models(db_conn=channel, query_model_vo=query_model_vo)

    def get_model_strategy(self, model_strategy, channel):
        """
        第二步，找到当前模型对应的策略
        :param model_strategy:
        :param channel:
        :return:
        """
        return self.model_strategy_manager.get_model_strategy(model_strategy=model_strategy, channel=channel)

    def initial(self, dispatcher_ctrl_parameter,  model_strategy,  channel):
        """
        第三步， 初始化调度信息
        :param dispatcher_ctrl_parameter:
        :param channel:
        :return:
        """
        work_date = dispatcher_ctrl_parameter.work_date
        if self.model_strategy_manager.check_model_strategy(model_strategy=model_strategy, work_date=work_date):
            self.dispatcher_manager.initial_dispatcher(dispatcher_ctrl_parameter=dispatcher_ctrl_parameter
                                                          , channel=channel)
        else:
            logger.debug(model_strategy.model_name + '下一个工作日不为' + work_date.strftime('%Y-%m-%d'))
        return
