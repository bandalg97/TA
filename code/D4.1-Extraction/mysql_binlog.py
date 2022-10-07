from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv

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
print("Connection established!")

order_events = []

for binlogevent in b_stream:
  for row in binlogevent.rows:
    if binlogevent.table == 'Orders':
      event = {}
      if isinstance(
            binlogevent, row_event.DeleteRowsEvent
        ):
        event["action"] = "delete"
        event.update(row["values"].items())
      elif isinstance(
            binlogevent, row_event.WriteRowsEvent
        ):
        event["action"] = "insert"
        event.update(row["values"].items())
      order_events.append(event)

b_stream.close()

keys = order_events[0].keys()
local_filename = './bucket/orders_log_extract.csv'

with open(
        local_filename,
        'w',
        newline='') as output_file:
    dict_writer = csv.DictWriter(
                output_file, keys,delimiter='|')
    dict_writer.writerows(order_events)

print("Logs are saved in your local directory!")