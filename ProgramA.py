from redis import Redis

File_dir = "name"
host_addr = "192.168.46.10"
list_name = "name"

class RedisObject():
    def __init__(self, host, port, db, red=None):
        self.host = host
        self.port = port
        self.db = db
        self.red = red
        if self.red is None:
            print("This Redis object have not connected.")

    def push_line_into_list(self, name, line):
        assert self.red is not None, "This Redis object have not connected."
        self.red.rpush(name, line)

    def pop_line_from_list(self, name):
        data = self.red.lpop(name)
        return data

    @staticmethod
    def redis_connected(host="localhost", port=6379, db=0):
        r = Redis(host=host, port=port, db=db)
        return RedisObject(host, port, db, r)

def read_data_from_file(file):
    f = open(file)
    line = f.readline()
    while line:
        yield line
        line = f.readline()

def push_FileData_into_RedisList(file):
    data = read_data_from_file(file)
    redis_object = RedisObject.redis_connected(host=host_addr)
    for d in data:
        if d is not None:
            redis_object.push_line_into_list(list_name, d)

if __name__ == '__main__':
    push_FileData_into_RedisList(File_dir)