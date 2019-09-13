import yaml
import pymongo
import random
from os.path import exists
import pickle

CONFIG_FILE = "config.yaml"
COMMON_PREFIX = "GGQMONGO"
BOUND = range(100, 200)  # 默认跳过的数量区间


def get_config() -> (list, list):
    """
    用来读取配置文件，配置文件中会标明要把哪几天的数据填充到另外哪几天
    :return:
    """
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config_data = yaml.load(f.read(), Loader=yaml.FullLoader)
    return config_data['from'], config_data['to']


def get_db_config():
    """
    用来读取mongo数据库的位置
    :return: 数据库的host以及port以及数据库名和集合名
    """
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config_data = yaml.load(f.read(), Loader=yaml.FullLoader)
    return config_data['db']


def read_from_data(key):
    """
    根据传入的Key拿取数据，如果本地有，就从本地读取，如果没有就从mongo中读取，然后返回
    :param key: 拿去数据的key
    :return: 根据key拿到的数据
    """
    key_file_name = f".{COMMON_PREFIX}{key}"
    # 如果本地有缓存的数据就从缓存中拿去数据
    if exists(key_file_name):
        with open(key_file_name, "rb") as f:
            return pickle.load(f)
    # 如果本地没有数据，就需要从mongo中读取，然后存储到本地
    mongo_data = [dd for dd in get_data_from_mongo(get_db_config(), key)]
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
    data = map(lambda x: update_func(x, "post_time", key), data)
    collect.insert(data)


def write_to_aim():
    from_keys, to_keys = get_config()
    all_from_data = [read_from_data(key) for key in from_keys]
    for to_key in to_keys:
        dd = random.choice(all_from_data)
        dd = random.sample(dd, len(dd) - random.choice(BOUND))
        write_to_mongo(get_db_config(), to_key, dd)


if __name__ == '__main__':
    write_to_aim()
