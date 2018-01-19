#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sourcemanager
import modelmanager
import dbmanager
import tablemanager
import platparametermanager
import extractmanager
import platformmanager
import log
import dispatcher
import dispatchermanager
import initialprocess
import sourceprocess
import modelstrategymanager
import inspectprocess
import extractprocess
import datetime
import const
import reviewprocess
import sys
import factory
import psutil

logger = log.get_logger()


def init(db_conn):
    sql = 'SELECT platform_id, source_manager_id, db_manager_id, model_manager_id, platform_manager_id' \
          ', strategy_manager_id FROM aaa_platform_config'
    cursor = db_conn.cursor()
    # log.debug(sql)
    # 查table_info
    cursor.execute(sql)
    rows = cursor.fetchall()
    platform_config_list = []
    # 每一条记录是一个配置策略
    for row in rows:
        platform_id = row[0]
        source_manager_id = row[1]
        db_manager_id = row[2]
        model_manager_id = row[3]
        platform_manager_id = row[4]
        strategy_manager_id = row[5]
        dic= {'platform_id': platform_id, 'source_manager_id': source_manager_id,
              'db_manager_id': db_manager_id, 'model_manager_id': model_manager_id,
              'platform_manager_id': platform_manager_id, 'strategy_manager_id': strategy_manager_id}
        platform_config_list.append(dic)

    ret_list = []
    # 针对找到的配置策略，关联出需要的详细信息
    for platform_config in platform_config_list:
        # 先取出必要的参数
        query_platform_manager_id = platform_config['platform_manager_id']
        query_model_manager_id = platform_config['model_manager_id']
        query_db_manager_id = platform_config['db_manager_id']
        query_source_manager_id = platform_config['source_manager_id']
        query_strategy_manager_id = platform_config['strategy_manager_id']

        # 配置取平台参数的方法为北京EDIP行为
        get_platform_behavior = platformmanager.GetPlatformParameterBehaviorBJEDIP()
        # 装配查询行为
        platform_manager = platformmanager.PlatformManager(get_platform_para_behavior=get_platform_behavior)
        # 设置查询vo
        platform_parameter_query_vo = platparametermanager.PlatformParameterFromDBForBJ(
            platform_manager_id=query_platform_manager_id)
        # 查询平台级设置
        platform_parameter_list = platform_manager.get_platform_parameter(
                                                                        platform_parameter=platform_parameter_query_vo,
                                                                        channel=db_conn)

        if platform_parameter_list is None or len(platform_parameter_list) != 1 :
            logger.error('根据配置信息查询平台级配置失败，extract程序无法正常启动')
            raise Exception('根据配置信息查询平台级配置失败，extract程序无法正常启动')
        # 获得平台级设置
        platform_parameter = platform_parameter_list[0]
        # to do to do to do
        # 这里应该是根据配置信息中的行为进行实例化，先缓缓
        source_para_behavior = platparametermanager.SourceParameterBehaviorBJEDIP()
        query_source_para_vo = platparametermanager.SourceParameterBJQueryVO(source_manager_id=query_source_manager_id)

        db_para_behavior = platparametermanager.DBParameterBehaviorBJEDIP()
        query_db_para_vo = platparametermanager.DBParameterBJEDIPQueryVO(db_manager_id=query_db_manager_id)

        model_para_behavior = platparametermanager.ModelParameterBehaviorBJEDIP()
        query_model_para_vo = platparametermanager.ModelExtractBJEDIPParameterQueryVO(model_manager_id=query_model_manager_id)

        strategy_para_behavior = platparametermanager.StrategyParameterBehaviorBJEDIP()
        query_strategy_para_vo = platparametermanager.StrategyParameterBJEDIP(strategy_manager_id=query_strategy_manager_id)

        # 整体的参数管理器
        parameter_manager = platparametermanager.ParameterManager(platform_parameter, source_para_behavior, db_para_behavior
                                                              , model_para_behavior, strategy_para_behavior)
        # 找到这个策略的数据源配置信息
        source_para_list = parameter_manager.get_source_manager_parameter(get_source_parameter=query_source_para_vo, channel=db_conn)
        if source_para_list is None or len(source_para_list) != 1:
            logger.error('根据配置信息查询数据源管理器配置失败，extract程序无法正常启动')
            raise Exception('根据配置信息查询数据源管理器配置失败，extract程序无法正常启动')
        source_para = source_para_list[0]

        db_para_list = parameter_manager.get_db_manager_parameter(get_db_parameter_vo=query_db_para_vo, channel=db_conn)
        if db_para_list is None or len(db_para_list) != 1 :
            logger.error('根据配置信息查询db管理器配置失败，extract程序无法正常启动')
            raise Exception('根据配置信息查询db挂利器配置失败，extract程序无法正常启动')
        db_para = db_para_list[0]

        model_para_list = parameter_manager.get_model_manager_parameter(get_model_parameter_vo=query_model_para_vo, channel=db_conn)
        if model_para_list is None or len(model_para_list) != 1 :
            logger.error('根据配置信息查询模型管理器参数配置失败，extract程序无法正常启动')
            raise Exception('根据配置信息查询模型管理器配置失败，extract程序无法正常启动')
        model_para = model_para_list[0]

        strategy_para_list = parameter_manager.get_strategy_manager_parameter(get_strategy_parameter_vo=query_strategy_para_vo, channel=db_conn)
        if strategy_para_list is None or len(strategy_para_list) != 1 :
            logger.error('根据配置信息查询平台策略管理器参数配置失败，extract程序无法正常启动')
            raise Exception('根据配置信息查询平台策略管理器配置失败，extract程序无法正常启动')
        strategy_para = strategy_para_list[0]
        parameter = platparametermanager.Parameter(source_parameter=source_para, db_parameter=db_para
                                               , model_parameter=model_para, strategy_parameter=strategy_para)
        ret_list.append(parameter)
    return ret_list


def assemble_model_manager(platform_parameter):
    # 构造注册参数
    register_model_parameter =  modelmanager.RegisterModelToMySQLParameter\
                                (database=platform_parameter.model_parameter.database_name \
                                , table_model_info=platform_parameter.model_parameter.model_info_table_name\
                                , table_column_info=platform_parameter.model_parameter.column_info_table_name)
    # 构造获取参数
    get_model_parameter = modelmanager.GetModelFromDBParameter\
                        (database=platform_parameter.model_parameter.database_name\
                        , table_model_info=platform_parameter.model_parameter.model_info_table_name\
                        , table_column_info=platform_parameter.model_parameter.column_info_table_name)
    # 按标准EDIP文件的方式创建
    generate_model_behavior = modelmanager.GenerateModelByStandardEDIPFileBehavior()
    # 按MySQL创建注册行为
    register_model_behavior = modelmanager.RegisterModelToMySQLBehavior(register_model_parameter)
    # 按从DB中获取方式创建获取行为
    get_model_behavior = modelmanager.GetModelFromDBBehavior(get_model_parameter)
    model_manager = modelmanager.ModelManger(generate_model_behavior, register_model_behavior, get_model_behavior)
    return model_manager


def assemble_dispatcher_manager(platform_parameter):
    add_dispatcher_behavior = dispatchermanager.AddDispatcherBehaviorBJEDIP(platform_parameter)
    get_dispatcher_behavior = dispatchermanager.GetDispatcherBehaviorBJEDIP(platform_parameter)
    upd_dispatcher_behavior = dispatchermanager.UpdDispatcherBehaviorBJEDIP(platform_parameter)
    dispatch_manager = dispatchermanager.DispatcherManager(add_dispatcher_behavior, get_dispatcher_behavior
                                                           , upd_dispatcher_behavior)
    return dispatch_manager


def assemble_model_strategy_manager(platform_parameter):
    register_model_strategy_behavior = modelstrategymanager.RegisterModelStrategyBehaviorBJEDIP()
    get_model_strategy_behavior = modelstrategymanager.GetModelStrategyBehaviorBJEDIP()
    update_model_strategy_behavior = modelstrategymanager.UpdateModelStrategyBehaviorBJEDIP()
    check_model_strategy_behavior = modelstrategymanager.CheckModelStrategyBehaivorBJEDIP()
    model_strategy_manager = modelstrategymanager.ModelStrategyManager(platform_parameter
                            , register_model_strategy_behavior
                            , get_model_strategy_behavior
                            , update_model_strategy_behavior
                            , check_model_strategy_behavior)
    return model_strategy_manager


def assemble_source_dispatcher(platform_parameter):
    source_dispatcher = sourceprocess.SourceDispatcherBJEDIP(platform_parameter)
    return source_dispatcher


def assemble_source_manager(platform_parameter):
    # 第一步：配置数据源管理器
    # 1 连接行为的配置
    # 1-1设定连接行为参数
    connect_parameter = sourcemanager.SFTPConnectParameter(address=platform_parameter.source_parameter.ip\
                                                           , port=platform_parameter.source_parameter.port\
                                                           , username=platform_parameter.source_parameter.user_name\
                                                           , password=platform_parameter.source_parameter.password)
    # 2 list行为的配置
    # 2-1 配置list参数
    list_parameter = sourcemanager.SFTPListParameter(remote_dir=platform_parameter.source_parameter.init_path)
    # 1-2 将连接参数装配给连接行为
    connect_behavior = sourcemanager.SFTPConnect(connect_parameter)
    # 2-2 将list参数配置给list行为
    list_behavior = sourcemanager.SFTPListBehavior(list_parameter)
    # 3 fetch行为的初始化
    fetch_behavior = sourcemanager.SFTPFetchBehavior()

    dispose_behavior = sourcemanager.SFTPDisposeBehavior()
    # 4 将行为实装给管理器
    source_manager = sourcemanager.SourceManager(connect_behavior, list_behavior, fetch_behavior, dispose_behavior)
    return source_manager


def assemble_db_manager(platform_parameter):
    db_parameter = platform_parameter.db_parameter
    db_mysql_parameter = dbmanager.DBMySQLParameter(database=db_parameter.database_name
                                                    , engine=db_parameter.mysql_engine
                                                    , charset=db_parameter.mysql_charset)
    db_gen_sql_behavior = dbmanager.DBMySQLGenSQLBehavior(db_mysql_parameter)
    db_manager = dbmanager.DBManager(db_gen_sql_behavior)
    return db_manager


def assemble_extract_dispatcher(platform_parameter):
    extract_dispatcher = extractprocess.ExtractDispatcherBJEDIP(platform_parameter)
    return extract_dispatcher


def assemble_extract_manager(platform_parameter, db_manager):
    # 装配extract管理器
    extract_behavior = extractmanager.ExtractEDIPFileBehavior(db_manager, platform_parameter)
    extract_manager = extractmanager.ExtractManager(db_manager, extract_behavior, platform_parameter)
    return extract_manager


def assemble_table_manager(platform_parameter, db_manager):
    create_behavior = tablemanager.CreateTableBehavior(db_manager)
    check_behavior = tablemanager.CheckTableBehavior(db_manager)
    table_manager = tablemanager.TableManager(create_behavior=create_behavior, check_behavior=check_behavior)
    return table_manager


def gen_dispatcher_para(model_strategy, work_date):
    model_strategy_id = model_strategy.model_strategy_id
    work_date = work_date
    model_name = model_strategy.model_name
    table_name = model_strategy.table_name
    status = 0
    disp_ctrl = dispatcher.DispatcherCtrlParameterBJ(model_strategy_id, work_date, status, model_name, table_name)
    return disp_ctrl


def disp(work_date, conn):
    test_platform_parameter = init(db_conn=conn)[0]
    model_manager = assemble_model_manager(test_platform_parameter)
    dispatch_manager = assemble_dispatcher_manager(test_platform_parameter)
    model_strategy_manager = assemble_model_strategy_manager(test_platform_parameter)
    db_manager = assemble_db_manager(test_platform_parameter)
    table_manager = assemble_table_manager(test_platform_parameter, db_manager)
    extract_dispatcher = assemble_extract_dispatcher(test_platform_parameter)
    extract_manager = assemble_extract_manager(test_platform_parameter, db_manager)

    initial_process = initialprocess.InitialProcessBJEDIP(model_manager=model_manager
                                                          , model_strategy_manager=model_strategy_manager
                                                          , dispatcher_manager=dispatch_manager)
    to_do_model_list_query_vo = modelstrategymanager.ModelStrategyBJEDIP(next_work_date=work_date, block_code=0)
    model_strategy_list_by_nx_date = model_strategy_manager.get_model_strategy(to_do_model_list_query_vo, channel=conn)
    done_set = set()
    for model_strategy_by_nx_date in model_strategy_list_by_nx_date:
        model_name_strategy = model_strategy_by_nx_date.model_name
        if model_name_strategy in done_set:
            continue
        done_set.add(model_name_strategy)
        query_model_vo = modelmanager.QueryModelFromDBVO(model_name=model_name_strategy, valid_date=work_date)
        #
        model_list = initial_process.get_work_models(query_model_vo=query_model_vo, channel=conn)
        #
        model = model_list[0]
        model_name = model.model_name
        # model.table_name = model
        model_strategy_query_vo = modelstrategymanager.ModelStrategyBJEDIP(model_name=model_name, table_name='', block_code=0)
        model_strategy_list = initial_process.get_model_strategy(model_strategy=model_strategy_query_vo, channel=conn)
        # 这里应该查是否已经存在了调度任务
        # 生成的查询dispathcer
        for model_strategy in  model_strategy_list:
            model.table_name = model_strategy.table_name
            dispatcher_ctrl_parameter_query_vo = gen_dispatcher_para(model_strategy, work_date)
            dispatcher_ctrl_parameter_query_vo.status = None
            initial_process.initial(dispatcher_ctrl_parameter_query_vo, model_strategy, conn)

            # 查询到需要的dispatcher
            dispatcher_ctrl_parameter_query_vo.status = const.dispatcher_status_initial
            list_dispatcher_ctrl_parameter = dispatch_manager.get_dispatcher(dispatcher_ctrl_parameter_query_vo, conn)
            for dispatcher_ctrl_parameter in list_dispatcher_ctrl_parameter:
                # 落地过程
                source_manager = assemble_source_manager(test_platform_parameter)
                source_dispatcher = assemble_source_dispatcher(test_platform_parameter)
                source_process = sourceprocess.SourceProcessBJEDIP(source_dispatcher=source_dispatcher
                                                                , source_manager=source_manager
                                                                , platform_parameter=test_platform_parameter)

                source_process.connect()
                source_process.source_download_initial(model_strategy, work_date, conn)
                source_process.source_download(model_strategy, work_date, conn)
                complete_value = source_process.source_complete(model_strategy, work_date, conn)
                source_process.dispose()

                if complete_value == 1:
                    dispatcher_ctrl_parameter.status = const.dispatcher_status_downloaded
                    dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
            conn.commit()

            dispatcher_ctrl_parameter_query_vo.status = const.dispatcher_status_downloaded
            list_dispatcher_ctrl_parameter = dispatch_manager.get_dispatcher(dispatcher_ctrl_parameter_query_vo, conn)

            for dispatcher_ctrl_parameter in list_dispatcher_ctrl_parameter:
                inspect_process = inspectprocess.InspectProcessBJEDIP(model_manager=model_manager,
                                                                      model_strategy_manager=model_strategy_manager,
                                                                      table_manager = table_manager,
                                                                      platform_parameter=test_platform_parameter)
                # 先验证是否注册
                result = inspect_process.inspect_model_registered(model_strategy=model_strategy, work_date=work_date, channel=conn)

                if result is True:
                    # 再验证数据日期
                    result = inspect_process.inspect_model_data_date(model_strategy=model_strategy, work_date=work_date, channel=conn)
                    if result is True:
                        # 再验证数据表是否一致
                        result = inspect_process.inspect_table_exist(model_strategy=model_strategy, work_date=work_date, channel=conn)
                        if result is True:
                            result = inspect_process.inspect_model_table_consistency(model_strategy=model_strategy, work_date=work_date, channel=conn)
                            if result is True:
                                dispatcher_ctrl_parameter.status = const.dispatcher_status_inspected
                                dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                            else:
                                # 模型不一致
                                dispatcher_ctrl_parameter.status = const.dispatcher_status_inspect_table_model_inconsistent
                                dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                                model_strategy.block_code = dispatcher_ctrl_parameter.status
                                model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                                             , channel=conn)
                        else:
                            # 模型不一致
                            dispatcher_ctrl_parameter.status = const.dispatcher_status_inspect_table_not_exist
                            dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                            model_strategy.block_code = dispatcher_ctrl_parameter.status
                            model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                                         , channel=conn)
                    else:
                        dispatcher_ctrl_parameter.status = const.dispatcher_status_inspect_wrong_data_date
                        dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                        # 如果被阻塞了，那么在策略信息里增加阻塞值
                        model_strategy.block_code = dispatcher_ctrl_parameter.status
                        model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                                     , channel=conn)
                else:
                    dispatcher_ctrl_parameter.status = const.dispatcher_status_inspect_unregistered
                    dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                    # 如果被阻塞了，那么在策略信息里增加阻塞值
                    model_strategy.block_code = dispatcher_ctrl_parameter.status
                    model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                                     , channel=conn)
            conn.commit()

            dispatcher_ctrl_parameter_query_vo.status = const.dispatcher_status_inspected
            list_dispatcher_ctrl_parameter = dispatch_manager.get_dispatcher(dispatcher_ctrl_parameter_query_vo, conn)

            for dispatcher_ctrl_parameter in list_dispatcher_ctrl_parameter:
                extract_process = extractprocess.ExtractProcessBJEDIP(extract_dispatcher=extract_dispatcher
                                                                     , extract_manager=extract_manager
                                                                     , db_manager=db_manager
                                                                     , model_manager=model_manager
                                                                     , platform_parameter=test_platform_parameter
                                                                     )
                extract_process.extract_initial(model_strategy=model_strategy
                                                , work_date=work_date, channel=conn)
                extract_process.extract(model_strategy=model_strategy, work_date=work_date, channel=conn)
                result = extract_process.extract_complete(model_strategy=model_strategy, work_date=work_date, channel=conn)
                if result is True:
                    model_strategy.data_date = work_date
                    model_strategy.next_work_date = model_strategy_manager.get_model_next_work_date(model_strategy)
                    model_strategy.block_code = 0
                    model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                        , channel=conn)
                    dispatcher_ctrl_parameter.status = const.dispatcher_status_extracted
                    dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
            conn.commit()

            dispatcher_ctrl_parameter_query_vo.status = const.dispatcher_status_extracted
            list_dispatcher_ctrl_parameter = dispatch_manager.get_dispatcher(dispatcher_ctrl_parameter_query_vo, conn)
            for dispatcher_ctrl_parameter in list_dispatcher_ctrl_parameter:
                review_process = reviewprocess.ReviewProcess(db_manager=db_manager, platform_parameter=test_platform_parameter)

                # 如果需要快照
                if reviewprocess.ReviewProcess.check_snap(model_strategy):
                    try:
                        # 如果是快照日
                        if reviewprocess.ReviewProcess.is_snap_date(work_date, model_strategy):
                            review_process.create_snap(work_date=work_date, model_strategy=model_strategy, channel=conn)
                    except Exception as e:
                        pass

                if reviewprocess.ReviewProcess.check_transform(model_strategy):
                    dispatcher_ctrl_parameter.status = const.dispatcher_status_review_need_transform
                    model_strategy.block_code = const.dispatcher_status_review_need_transform
                    dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                    model_strategy_manager.update_model_strategy(model_strategy=model_strategy
                                                        , channel=conn)
                else:
                    dispatcher_ctrl_parameter.status = const.dispatcher_status_complete
                    dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
                dispatcher_ctrl_parameter.status = const.dispatcher_status_complete
                dispatch_manager.upd_dispatcher(dispatcher_ctrl_parameter, conn)
            conn.commit()


def check_process_exist():
    process_is_exist = False
    process_times = 0
    for proc in psutil.process_iter():
        pinfo = proc.as_dict(attrs=['name','cmdline'])
        if pinfo.get('name') == 'python3':
            list = pinfo.get('cmdline')
            for cmdpara in list:
                if cmdpara == '/etl/main.py':
                    process_times = process_times + 1
                    logger.debug('发现main进程，进程数量为:%d'%process_times)
                    if process_times > 1:
                        process_is_exist = True
                        break
    return process_is_exist






def main():
    args_len = len(sys.argv)
    logger.info('输入参数数量为%d'%(args_len))
    if args_len ==3:
        logger.info('输入参数为%s,%s'%(sys.argv[1], sys.argv[2]))
        start_date_str = sys.argv[1]
        end_date_str = sys.argv[2]
        start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d').date()
        end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d').date()
        conn = factory.get_conn()
        while start_date<=end_date:
            disp(work_date=start_date, conn=conn)
            start_date = start_date + datetime.timedelta(days=1)
        conn.close()
    elif args_len == 1:
        now_date = datetime.datetime.now().date()
        start_date = now_date + datetime.timedelta(days=-1)
        conn = factory.get_conn()
        disp(work_date=start_date, conn=conn)
    else:
        return

is_exist = check_process_exist()
if is_exist is True:
    logger.error('进程存在，程序即将退出')
    exit()
else:
    logger.info('进程不存在，将进入主进程')
    main()


