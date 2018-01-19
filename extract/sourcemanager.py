# -*- coding: utf-8 -*-

__author__ = 'tdanieln@gmail.com'
import os
import re
import paramiko
import ftplib
import log

logger = log.get_logger()


class SourceManager(object):
    # 数据源管理器
    """

    """
    def __init__(self, connect_behavior, list_behavior, fetch_behavior, dispose_behavior):
        # 构造方法
        """
        装配连接行为，列表行为，获取行为
        """
        self.connect_behavior = connect_behavior
        self.list_behavior = list_behavior
        self.fetch_behavior = fetch_behavior
        self.dispose_behavior = dispose_behavior
        self.channel = None

    def connect(self):
        # 连接方法
        self.channel = self.connect_behavior.connect()

    def dispose(self):
        # 析构方法
        if self.channel is not None:
            self.dispose_behavior.dispose(self.channel)

    def list(self):
        # 列表方法，用于获取数据源信息列表
        return self.list_behavior.list(self.channel)

    def fetch(self, fetch_parameter):
        # 获取方法，用于真正的捕获数据
        return self.fetch_behavior.fetch(self.channel, fetch_parameter)

    def clean(self):
        pass


class ConnectBehavior(object):
    # 连接行为
    """

    """
    def connect(self):
        pass


class SFTPConnect(ConnectBehavior):
    # 使用SFTP的方式实现的连接行为
    def __init__(self, sftp_connect_para):
        if isinstance(sftp_connect_para, SFTPConnectParameter) is False:
            raise Exception('使用SFTP参数连接时，需要送入SFTP连接参数,参数类型为SFTPConnectParameter')
        self.address = sftp_connect_para.address
        self.port = sftp_connect_para.port
        self.username = sftp_connect_para.username
        self.password = sftp_connect_para.password

    def connect(self):
        transport = paramiko.Transport(self.address, self.port)
        transport.connect(username=self.username, password=self.password)
        # sftp = paramiko.SFTPClient.from_transport(transport)
        return transport


class FTPConnect(ConnectBehavior):
    # 使用FTP的方式实现的连接行为
    def __init__(self, ftp_connect_para):
        if isinstance(ftp_connect_para, FTPConnectParameter) is False:
            raise Exception('使用FTP参数连接时，需要送入FTP连接参数,参数类型为FTPConnectParameter')
        self.address = ftp_connect_para.address
        self.port = ftp_connect_para.port
        self.username = ftp_connect_para.username
        self.password = ftp_connect_para.password

    def connect(self):
        ftp = ftplib.FTP()
        ftp.connect(host=self.address,port=self.port)
        ftp.login(user=self.username, passwd=self.password)
        return ftp


class ConnectParameter():
    # 连接参数
    def __init__(self):
        pass


class SFTPConnectParameter(ConnectParameter):
    # SFTP需要的连接参数
    def __init__(self, address='127.0.0.1', port=22, username='anonymous', password='anonymous'):
        self.address = address
        self.port = port
        self.username = username
        self.password = password


class FTPConnectParameter(ConnectParameter):
    # FTP需要的连接参数
    def __init__(self, address='127.0.0.1', port=21, username='anonymous', password='anonymous'):
        self.address = address
        self.port = port
        self.username = username
        self.password = password


class ListBehavior(object):
    # 展示行为

    def __init__(self, list_parameter):
        # 构造函数
        pass

    def list(self, channel):
        # 展示行为
        pass


class SFTPListBehavior(ListBehavior):
    # 使用SFTP方式实现List行为
    def __init__(self, sftp_list_parameter):
        self.remote_dir = sftp_list_parameter.remote_dir

    def list(self, channel):
        files = channel.listdir(self.remote_dir)
        return files


class FTPListBehavior(ListBehavior):
    def __init__(self, ftp_list_parameter):
        self.remote_dir = ftp_list_parameter.remote_dir
        pass

    def list(self, channel):
        channel.cwd(self.remote_dir)
        files = channel.nlst()
        return files


class FTPOldListBehavior(FTPListBehavior):
    def list(self, channel):
        channel.cwd(self.remote_dir)
        ftp_file_name_list = []
        channel.retrlines('LIST', ftp_file_name_list.append)
        return ftp_file_name_list


class ListParameter(object):
    pass


class SFTPListParameter(ListParameter):
    def __init__(self, remote_dir='./', filename_pattern=''):
        self.remote_dir = remote_dir
        self.filename_pattern = filename_pattern


class FTPListParameter(ListParameter):
    def __init__(self, remote_dir='./'):
        self.remote_dir = remote_dir


class FetchBehavior(object):
    def fetch(self, channel, fetch_content):
        pass


class SFTPFetchBehavior(FetchBehavior):

    def fetch(self, channel, fetch_content_file_parameter):
        if channel is None:
            logger.error('使用SFTP方式获取文件时，通道参数为空，请检查连接设置')
            raise Exception('使用SFTP方式获取文件时，通道参数为空，请检查连接设置')
        if isinstance(channel, paramiko.transport.Transport) is False:
            logger.error('使用SFTP方式获取文件时，通道参数不为paramiko.transport.Transport，请检查')
            raise Exception('使用SFTP方式获取文件时，通道参数不为paramiko.transport.Transport，请检查')

        if (isinstance(fetch_content_file_parameter, FetchContentSingleFileParameter) is False) and \
                (isinstance(fetch_content_file_parameter, FetchContentBatchFileParameter) is False):
            logger.error('使用SFTP方式获取文件时，获取内容参数必须为单个文件参数或者批量参数')
            raise Exception('使用SFTP方式获取文件时，获取内容参数必须为单个文件参数或者批量参数')

        fetch_success_file = []

        sftp_channel = paramiko.SFTPClient.from_transport(channel)
        # 如果是批量参数
        if isinstance(fetch_content_file_parameter, FetchContentBatchFileParameter):
            fetch_list = fetch_content_file_parameter.fetch_list
            remote_dir = fetch_content_file_parameter.remote_dir
            local_dir = fetch_content_file_parameter.local_dir
            for f in fetch_list:
                # 这里本应该的写法是remote_file = os.path.join(remote_dir, f)
                remote_file = remote_dir + '/' + f
                local_file = os.path.join(local_dir, f)
                try:
                    sftp_channel.get(remote_file, local_file)
                except Exception as e:
                    logger.error('从远端文件夹%s获取文件%s失败,本地目标文件为%s'%(remote_dir, remote_file, local_file))
                    logger.error(repr(e))
                    continue
                fetch_success_file.append(f)
        else:
            # 如果是单笔参数
            remote_file = fetch_content_file_parameter.remote_file
            local_file = fetch_content_file_parameter.local_file
            pattern_str = r'([\|/]\w*)*([\|/]\w*\..*)'
            m = re.match(pattern_str, remote_file)
            remote_file_name = remote_file
            if m is not None:
                remote_file_name = m.group(2)[1:]
            try:
                sftp_channel.get(remote_file, local_file)
            except Exception as e:
                logger.error('从远端获取文件%s失败'%remote_file)
                logger.error(repr(e))
            fetch_success_file.append(remote_file_name)

        return fetch_success_file


class FTPFetchBehavior(FetchBehavior):
    def fetch(self, channel, fetch_content_file_parameter):
        if channel is None:
            logger.error('使用FTP方式获取文件时，通道参数为空，请检查连接设置')
            raise Exception('使用FTP方式获取文件时，通道参数为空，请检查连接设置')

        if isinstance(channel, ftplib.FTP) is False:
            logger.error('使用FTP方式获取文件时，通道参数不为ftplib.FTP，请检查')
            raise Exception('使用FTP方式获取文件时，通道参数不为ftplib.FTP，请检查')

        if (isinstance(fetch_content_file_parameter, FetchContentSingleFileParameter) is False) and \
                (isinstance(fetch_content_file_parameter, FetchContentBatchFileParameter) is False):
            logger.error('使用FTP方式获取文件时，获取内容参数必须为单个文件参数或者批量参数')
            raise Exception('使用FTP方式获取文件时，获取内容参数必须为单个文件参数或者批量参数')

        fetch_success_file = []
        # 如果是批量参数
        if isinstance(fetch_content_file_parameter, FetchContentBatchFileParameter):
            fetch_list = fetch_content_file_parameter.fetch_list
            remote_dir = fetch_content_file_parameter.remote_dir
            local_dir = fetch_content_file_parameter.local_dir
            for f in fetch_list:
                # 这里本应该的写法是remote_file = os.path.join(remote_dir, f)
                remote_file = remote_dir + '/' + f
                local_file = os.path.join(local_dir, f)
                try:
                    with open(local_file, mode='wb') as wf:
                        channel.retrbinary('RETR ' + remote_file, wf.write, 1024)
                except Exception as e:
                    logger.error(repr(e))
                    logger.error('从远端文件夹%s获取文件%s失败,本地目标文件为%s'%(remote_dir, remote_file, local_file))
                    continue
                fetch_success_file.append(f)
        else:
            # 如果是单笔参数
            remote_file = fetch_content_file_parameter.remote_file
            local_file = fetch_content_file_parameter.local_file
            pattern_str = r'([\|/]\w*)*([\|/]\w*\..*)'
            m = re.match(pattern_str, remote_file)
            remote_file_name = remote_file
            if m is not None:
                remote_file_name = m.group(2)[1:]
            try:
                with open(local_file, mode='wb') as wf:
                    channel.retrbinary('RETR ' + remote_file, wf.write, 1024)
            except Exception as e:
                logger.error(repr(e))
                logger.error('从远端获取文件%s失败'%remote_file)
            fetch_success_file.append(remote_file_name)

        return fetch_success_file


class FetchContentParameter(object):
    pass


class FetchContentBatchFileParameter(FetchContentParameter):
    def __init__(self,  remote_dir, local_dir, fetch_list):
        self.remote_dir = remote_dir
        self.local_dir = local_dir
        self.fetch_list = fetch_list


class FetchContentSingleFileParameter(FetchContentParameter):
    def __init__(self, remote_file, local_file=''):
        self.remote_file = remote_file
        self.local_file = local_file


class DisposeBehavior(object):
    def __init__(self):
        pass

    def dispose(self, channel):
        pass


class SFTPDisposeBehavior(DisposeBehavior):
    def __init__(self):
        pass

    def dispose(self, channel):
        channel.close()




















