#!/usr/bin/env python
# encoding: utf-8
import uuid
import math
import time
import string
import random
import secrets
import json
import dicttoxml
import socket
import struct


# top100中文字符
CN_TOP100_STRING = '的一国在人了有中是年和大业不为发会工经上地市要个产这出行作生家以成到日民来我部对进多全建他公开们场展时理新方主企资实学报制政济用同于法高长现本月定化加动合品重关机分力自外者区能设后就等体下万元社过前面'

# 随机时间有效范围
START_TIME = time.mktime((1990, 1, 1, 0, 0, 0, 0, 0, 0))
END_TIME = time.mktime((2030, 1, 1, 0, 0, 0, 0, 0, 0))

# 随机IP规则
RANDOM_IP_POOL = ['192.168.10.222/0']

# 用于生成自增且唯一数据
CURRENT_UNIQUE_NUM = 0
MAX_UNIQUE_NUM = 999999


def random_sorted_and_unique_num():
    """
    有效范围内随机生成有序且唯一数字
    """
    global CURRENT_UNIQUE_NUM
    random_num = str(int(round(time.time() * 1)))
    if CURRENT_UNIQUE_NUM > MAX_UNIQUE_NUM:
        CURRENT_UNIQUE_NUM = 0
    random_num = '{0}{1}'.format(random_num, str(CURRENT_UNIQUE_NUM).zfill(6))
    CURRENT_UNIQUE_NUM += 1
    return int(random_num)


def random_int(from_num, end_num):
    """
    有效范围内随机int
    """
    from_num = int(from_num)
    end_num = int(end_num)
    return random.randint(from_num, end_num)


def random_decimal(precision, scale, from_num, end_num, unsigned):
    """
    有效范围内随机decimal
    """
    scale = int(scale)
    precision = int(precision)
    int_count = precision - scale
    from_num = int(from_num)
    end_num = int(end_num)
    unsigned = bool(unsigned)
    scale_format = '{:.' + str(scale) + 'f}'
    if not unsigned:
        if int_count == 0:
            random_num = random.uniform(-1, 1)
        else:
            random_num = random.uniform(from_num, end_num)
    else:
        if int_count == 0:
            random_num = random.uniform(0, 1)
        else:
            random_num = random.uniform(0, end_num)
    return scale_format.format(random_num)


def random_str(precision):
    """
    有效范围内随机str
    """
    lines = []
    precision = int(precision)
    precision = random_int(1, precision)
    sampler = string.digits + string.ascii_letters + CN_TOP100_STRING
    while True:
        if precision <= 100:
            lines.append(''.join(random.sample(sampler, precision)))
            break
        lines.append(''.join(random.sample(sampler, 100)))
        precision = precision - 100
    return ''.join(lines)


def random_str_without_punctuation(precision):
    """
    有效范围内随机str，且没有标点符号
    """
    lines = []
    precision = int(precision)
    precision = random_int(1, precision)
    sampler = string.digits + string.ascii_letters + CN_TOP100_STRING
    while True:
        if precision <= 100:
            lines.append(''.join(random.sample(sampler, precision)))
            break
        lines.append(''.join(random.sample(sampler, 100)))
        precision = precision - 100
    return ''.join(lines)


def random_str_without_cn_punctuation(precision):
    """
    有效范围内随机str，且没有标点符号、中文
    """
    lines = []
    precision = int(precision)
    precision = random_int(1, precision)
    sampler = string.digits + string.ascii_letters
    while True:
        if precision <= 50:
            lines.append(''.join(random.sample(sampler, precision)))
            break
        lines.append(''.join(random.sample(sampler, 50)))
        precision = precision - 50
    return ''.join(lines)


def random_str_which_cn_nbytes(precision, n_bytes=2):
    """
    有效范围内随机str，且中文占2个字节
    """
    lines = []
    precision = int(precision)
    precision = random_int(1, precision)
    if precision <= n_bytes:
        sampler = string.digits + string.ascii_letters
        lines.append(''.join(random.sample(sampler, precision)))
        return ''.join(lines)
    else:
        precision_ch = int(precision / 3)
        sampler1 = CN_TOP100_STRING
        sampler2 = string.digits + string.ascii_letters
        while True:
            if precision_ch <= 100:
                lines.append(''.join(random.sample(sampler1, precision_ch)))
                precision = precision - precision_ch * n_bytes
                break
            lines.append(''.join(random.sample(sampler1, 100)))
            precision = precision - 100 * n_bytes
            precision_ch = precision_ch - 100
        while True:
            if precision <= 50:
                lines.append(''.join(random.sample(sampler2, precision)))
                break
            lines.append(''.join(random.sample(sampler2, 50)))
            precision = precision - 50
        return ''.join(lines)


def random_bytes(precision):
    """
    有效范围内随机bytes
    """
    precision = random.randint(1, precision)
    return secrets.token_bytes(precision)


def random_date(format_str="%Y-%m-%d"):
    """
    有效范围内随机date
    """
    date_tuple = time.localtime(random.randint(START_TIME, END_TIME))
    date = time.strftime(format_str, date_tuple)
    return date


def random_time(format_str="%H:%M:%S"):
    """
    有效范围内随机time
    """
    date_tuple = time.localtime(random.randint(START_TIME, END_TIME))
    date = time.strftime(format_str, date_tuple)
    return date


def random_year(format_str="%Y"):
    """
    有效范围内随机year
    """
    date_tuple = time.localtime(random.randint(START_TIME, END_TIME))
    date = time.strftime(format_str, date_tuple)
    return date


def random_datetime(format_str="%Y-%m-%d %H:%M:%S"):
    """
    有效范围内随机datetime
    """
    date_tuple = time.localtime(random.randint(START_TIME, END_TIME))
    date = time.strftime(format_str, date_tuple)
    return date


def random_timestamp(format_str="%Y-%m-%d %H:%M:%S"):
    """
    有效范围内随机timespan
    """
    date_tuple = time.localtime(random.randint(START_TIME, END_TIME))
    date = time.strftime(format_str, date_tuple)
    return date


def random_bit(precision):
    """
    有效范围内随机bit
    """
    return random_int(0, math.pow(2, precision - 1))


def random_point():
    """
    有效范围内随机point
    """
    geometry = [random_int(1, 50), random_int(1, 50)]
    return geometry


def random_multipoint():
    """
    有效范围内随机multipoint
    """
    geometry = [random_int(1, 100), random_int(1, 100), random_int(1, 100), random_int(1, 100)]
    return geometry


def random_linestring():
    """
    有效范围内随机linestring
    """
    geometry = [random_int(1, 100), random_int(1, 100), random_int(1, 100), random_int(1, 100)]
    return geometry


def random_multilinestring():
    """
    有效范围内随机multilinestring
    """
    geometry = [random_int(1, 100), random_int(1, 100), random_int(1, 100), random_int(1, 100),
                random_int(1, 100), random_int(1, 100), random_int(1, 100), random_int(1, 100)]
    return geometry


def random_polygon():
    """
    有效范围内随机polygon
    """
    p1 = random_int(1, 100)
    p2 = random_int(1, 100)
    p3 = random_int(1, 100)
    p4 = random_int(1, 100)
    p5 = random_int(1, 100)
    p6 = random_int(1, 100)
    p7 = random_int(1, 100)
    p8 = random_int(1, 100)
    geometry = [p1, p2, p3, p4, p5, p6, p7, p8, p1, p2]
    return geometry


def random_multipolygon():
    """
    有效范围内随机multipolygon
    """
    p1 = random_int(1, 100)
    p2 = random_int(1, 100)
    p3 = random_int(1, 100)
    p4 = random_int(1, 100)
    p5 = random_int(1, 100)
    p6 = random_int(1, 100)
    p7 = random_int(1, 100)
    p8 = random_int(1, 100)
    geometry = [p1, p2, p3, p4, p5, p6, p7, p8, p1, p2]
    p1 = random_int(1, 100)
    p2 = random_int(1, 100)
    p3 = random_int(1, 100)
    p4 = random_int(1, 100)
    p5 = random_int(1, 100)
    p6 = random_int(1, 100)
    p7 = random_int(1, 100)
    p8 = random_int(1, 100)
    geometry.extend([p1, p2, p3, p4, p5, p6, p7, p8, p1, p2])
    return geometry


def random_json():
    """
    有效范围内随机json
    """
    json_value = {
        'Fix_Data': {
            'str': 'TEST_DATA',
            'int': 100,
            'float': 999.11,
            'null': None,
            'bool': True,
        },
        'Random_Data':
            [
                str(uuid.uuid4()),
                ''.join(random.sample(string.digits + string.ascii_letters + CN_TOP100_STRING, 10))
            ]
    }
    return json.dumps(json_value)


def random_xml():
    """
    有效范围内随机xml
    """
    json_value = \
        {
            'DP':
                {
                    'Fix_Data': {
                        'str': 'TEST_DATA',
                        'int': 100,
                        'float': 999.11,
                        'null': None,
                        'bool': True,
                    },
                    'Random_Data':
                        [
                            str(uuid.uuid4()),
                            ''.join(random.sample(string.digits + string.ascii_letters + CN_TOP100_STRING, 10))
                        ]
                }
        }
    return dicttoxml.dicttoxml(json_value, root=False).decode("utf-8")


def random_uuid():
    """
    有效范围内随机uuid
    """
    return uuid.uuid4()


def random_mac():
    """
    有效范围内随机mac
    """
    mac = [0x00, 0x16, 0x3e,
           random_int(0x00, 0x7f),
           random_int(0x00, 0xff),
           random_int(0x00, 0xff)]
    return ':'.join(map(lambda x: "%02x" % x, mac))


def random_ip():
    """
    有效范围内随机ip
    """
    str_ip = RANDOM_IP_POOL[random.randint(0, len(RANDOM_IP_POOL) - 1)]
    str_ip_addr = str_ip.split('/')[0]
    str_ip_mask = str_ip.split('/')[1]
    ip_addr = struct.unpack('>I', socket.inet_aton(str_ip_addr))[0]
    mask = 0x0
    for i in range(31, 31 - int(str_ip_mask), -1):
        mask = mask | (1 << i)
    ip_addr_min = ip_addr & (mask & 0xffffffff)
    ip_addr_max = ip_addr | (~mask & 0xffffffff)
    return socket.inet_ntoa(struct.pack('>I', random.randint(ip_addr_min, ip_addr_max)))
