from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication

# get the MySQL connection info
parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")

mysql_settings = {
    "host": hostname,
    "port": int(port),
    "user": username
}

b_stream = BinLogStreamReader(
            connection_settings = mysql_settings,
            server_id=100,
            only_events=[row_event.DeleteRowsEvent,
                        row_event.WriteRowsEvent]
            )

for event in b_stream:
    event.dump()

b_stream.close()