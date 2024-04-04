import pymysql
from sshtunnel import SSHTunnelForwarder


class MysqlServer:
    def __init__(self, db_config):
        self.db_config = db_config
        self.cursor = None

    def __enter__(self):
        if self.db_config.get("ssh_tunnel"):
            self.is_ssh = True
            # 使用ssh tunnel建立连接
            ssh_config = self.db_config.get("ssh_tunnel")
            self.tunnel_server = SSHTunnelForwarder(
                (ssh_config['host'], ssh_config['port']),
                ssh_username=ssh_config['username'],
                ssh_pkey=ssh_config['pem_file_path'],
                remote_bind_address=(ssh_config['remote_bind_address']['host'], ssh_config['remote_bind_address']['port'])
            )
            self.tunnel_server.start()
            db = pymysql.connect(port=self.tunnel_server.local_bind_port, **self.db_config['mysql'])
        else:
            self.is_ssh = False
            db = pymysql.connect(**self.db_config['mysql'])
        self.cursor = db.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if self.is_ssh:
            self.tunnel_server.close()

