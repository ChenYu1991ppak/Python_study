from pymongo import MongoClient
from ProgramA import RedisObject

host_addr = "192.168.46.10"
port = 27017
usr = None
passwd = None
list_name = "name"
database = "geetest_python"
collection = "names"

class MonClient():
    def __init__(self, host=host_addr, p=port, u=None, pwd=None):
        self.host = host
        self.port = p
        self.mongo = MongoClient(self.host, self.port)
        self.db = None
        self.usr = u
        self.pwd = pwd

    def select_database(self, database):
        self.db = self.mongo[database]
        if self.usr is not None and self.pwd is not None:
            self.db.authenticate(self.usr, self.pwd)

    def write_into_database(self, collection, data):
        record = {"name": data}
        col = self.db.get_collection(collection)
        col.insert_one(record).inserted_id

def pop_data_from_redis(addr):
    re = RedisObject.redis_connected(host=addr)
    data = re.pop_line_from_list(list_name)
    while data:
        yield data
        data = re.pop_line_from_list(list_name)

if __name__ == '__main__':
    Client = MonClient(u=usr, pwd=passwd)
    Client.select_database(database)
    data = pop_data_from_redis(host_addr)
    for d in data:
        if d is not None:
            Client.write_into_database(collection, d)


