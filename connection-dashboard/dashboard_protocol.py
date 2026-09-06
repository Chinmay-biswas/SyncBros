import json 
from datetime import datetime 

class ApiError(ValueError):
    """ error"""


def now_label():
    return datetime.now().astimezone().strftime("%d %b, %I:%M %p")

def send_message(connection, message):
    connection.sendall(json.dumps(message, seprators=(",", ":")).encode("utf-8"))

def read_message(reader):
    line=reader.readline(24*1024*1024+1)
    if not line:
        raise ConnectionError("peer disconnected")
    if len(line) > 24*1024*1024:
        raise ApiError("message too long")
    message=json.loads(line)
    if not isinstance(message,dict):
        raise ApiError("expected a JSON object")
    return message