#!/usr/bin/env python
# encoding: utf-8
import os
import re
import json
import openpyxl
from collections import OrderedDict


def get_xls(root, excel_name, sheet_name):
    """
    读取Excel文件，sheet页内容
    """
    cls = []
    excel_path = os.path.abspath(os.path.join(root, excel_name))
    wb = openpyxl.load_workbook(excel_path)
    sheet = wb[sheet_name]
    rows = sheet.rows
    for row in rows:
        columns = [cell.value for cell in row]
        cls.append(columns)
    return cls


def format_json(ori_json, lower_key=False, encoding='utf-8'):
    # 处理每一行数据，使用\n分割进行单独处理
    lines = ori_json.split('\n')
    new_lines = []
    for line in lines:
        # 替换\t
        line = line.replace('\t', '\\t')
        # 匹配是否为k:v格式，如匹配则将v值中的双引号去除
        search_result = re.search(r'^  "([^"]*)": "(.*)",', line)
        if search_result:
            col_name = search_result.group(1)
            col_value = search_result.group(2)
            line = '  "' + col_name + '": "' + col_value.replace('"', '\'') + '",'
        new_lines.append(line)
    msg = ''.join(new_lines)
    result_dict = json.loads(msg, encoding=encoding, object_pairs_hook=OrderedDict)
    if lower_key:
        result_dict = {k.lower(): v for k, v in result_dict.items()}
    return result_dict
