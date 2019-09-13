import yaml
import pymongo
import random
from os.path import exists
import pickle
import logging
import tqdm
import base64

logging.basicConfig(level=logging.INFO)
CONFIG_FILE = "config.yaml"
BOUND = range(3000, 6000)  # 随机对数据进行减少，防止数据都一样


class ConfigObj:
    obj = None

    def __init__(self):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            self.obj = yaml.load(f.read(), Loader=yaml.FullLoader)

    def get_db(self):
        return self.obj['db']

    def get_keys(self):
        return self.obj['from'], self.obj['to']

    def get_field_name(self):
        return self.obj['field_name'] if self.obj['field_name'] is not None else "post_time"

    def get(self, k: str):
        return self.obj[k]


obj = ConfigObj()


def read_from_data(key):
    """
    根据传入的Key拿取数据，如果本地有，就从本地读取，如果没有就从mongo中读取，然后返回
    :param key: 拿去数据的key
    :return: 根据key拿到的数据
    """
    db_config = obj.get_db()
    flag = str(base64.b64encode(
        f"{db_config['host']}:{db_config['port']}{db_config['name']}{db_config['collectionname']}".encode("utf-8")),
        "utf-8")
    key_file_name = f".{flag}-{key}"
    # 如果本地有缓存的数据就从缓存中拿去数据
    if exists(key_file_name):
        with open(key_file_name, "rb") as f:
            return pickle.load(f)
    # 如果本地没有数据，就需要从mongo中读取，然后存储到本地
    mongo_data = [dd.copy() for dd in get_data_from_mongo(db_config, key)]
    with open(key_file_name, "wb") as f:
        pickle.dump(mongo_data, f)
    return mongo_data


def get_data_from_mongo(db, key):
    """
    根据Key从mongo中读取数据
    :param db: 连接的mongo的信息
    :param key: 拿取数据的Key
    :return: 拿取的数据
    """
    client = pymongo.MongoClient(db['host'], db['port'])
    collect = client.get_database(db['name']).get_collection(db['collectionname'])
    data = collect.find(
        filter={"post_time": key},
        projection={'_id': False},
        # skip=random.randint(*BOUND),
        batch_size=500,
    )
    return data


def update_func(x, key, value):
    x.update({key: value})
    return x


def write_to_mongo(db, key, data):
    """
    根据db的配置以及key写入数据
    :param db: 数据库配置
    :param key: 要写入的key
    :param data: 要写入的数据
    :return:
    """
    client = pymongo.MongoClient(db['host'], db['port'])
    collect = client.get_database(db['name']).get_collection(db['collectionname'])
    write_data = map(lambda x: update_func(x, obj.get_field_name(), key), data)
    collect.insert_many(write_data)


def write_to_aim():
    logging.info("加载配置文件")
    from_keys, to_keys = obj.get_keys()
    logging.info(f"从{from_keys}填充到{to_keys}")
    logging.info("开始写入数据")
    for to_key in tqdm.tqdm(to_keys):
        from_key = random.choice(from_keys)
        dd = read_from_data(from_key)
        write_len = len(dd) - random.choice(BOUND)
        while write_len > 0:
            logging.info(f"当前选择的key对应的数据长度过小!当前key为{from_key}，考虑删除掉此key")
            from_key = random.choice(from_keys)
            dd = read_from_data(from_key)
            write_len = len(dd) - random.choice(BOUND)
        final_dd = random.sample(dd, write_len).copy()
        logging.info(f"向{to_key}写入{write_len}条数据")
        write_to_mongo(obj.get_db(), to_key, final_dd)
        logging.info(f"写入{to_key}完成")
        del dd


if __name__ == '__main__':
    write_to_aim()
