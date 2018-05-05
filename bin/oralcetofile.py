#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 数据库常用连接服务

import db_connect
import argparse
import os
import datetime
from multiprocessing import Pool

try:
    import xml.etree.cElementTree as etree
except ImportError:
    import xml.etree.ElementTree as etree

parser = argparse.ArgumentParser()
parser.add_argument('--group', dest='group', required=True)
parser.add_argument('--procnum', dest='procnum', required=True)
args = parser.parse_args()
group = args.group
procnum = int(args.procnum)
file_path = os.path.realpath(__file__)
bin_path = os.path.split(file_path)[0]
base_path = os.path.split(bin_path)[0]
con_path = base_path + os.sep + 'config'
data_path = base_path + os.sep + 'data'
db_cfg_file = con_path + os.sep + 'db_config.xml'
unload_cfg_file = con_path + os.sep + 'unload_cfg.xml'


def table_unload(dbname, tablename, query_sql):
    print('Unload database %s table %s begin (%s)' % (dbname, tablename, os.getpid()))
    db_config = db_connect.get_db_config(db_cfg_file, dbname)
    db_conn = db_connect.get_connect(db_config)
    db_cursor = db_conn.cursor()
    if query_sql == '':
        query_sql = 'select * from %s' % (tablename)
    data_file_name = tablename + '.csv'
    data_file = data_path + os.sep + data_file_name
    open_data_file = open(data_file, 'w', encoding='utf8')
    db_cursor.execute(query_sql)
    while 1:
        eachline = db_cursor.fetchone()
        if eachline is None:
            break
        else:
            outline = []
            for eachcar in eachline:
                line_str = str(eachcar)
                if line_str == 'None':
                    line_str = ''
                outline.append(line_str)
            spilt_chr = '@@'
            line_str = spilt_chr.join(outline)
            open_data_file.write(line_str + '\n')
    db_cursor.close()
    db_conn.close()
    print('Unload database %s table %s end (%s)' % (dbname, tablename, os.getpid()))
    '''
    result = db_cursor.fetchall()
    for eachline in result:
        outline=[]
        for eachcar in eachline:
            line_str=str(eachcar)
            if line_str == 'None':
                line_str=''
            outline.append(line_str)
        spilt_chr='@@'
        line_str = spilt_chr.join(outline)
        open_data_file.write(line_str+'\n')
    db_cursor.close()
    db_conn.close()
    print('Unload database %s table %s end (%s)' % (dbname, tablename, os.getpid()))
    '''


# print(db_cfg_file);
if __name__ == '__main__':
    mainStart = datetime.datetime.now()
    print('Time:[%s] Start the main process (%s).' % (mainStart,os.getpid()))
    p = Pool(procnum)
    unload_tree = etree.parse(unload_cfg_file)
    group_tree = unload_tree.find('GROUP[@ID="%s"]' % group)
    for db_tree in group_tree:
        db_name = db_tree.attrib['NAME']
        for table_tree in db_tree:
            table_name = table_tree.findtext('NAME')
            table_query_sql = table_tree.findtext('QUERY_SQL')
            p.apply_async(table_unload, args=(db_name, table_name, table_query_sql,))
    p.close()
    p.join()
    print('All subprocesses done')
    mainEnd = datetime.datetime.now()
    print('Time:[%s] End the main process (%s).' % (mainEnd, os.getpid()))
    print('All process run %0.2f seconds.' % (mainEnd - mainStart).seconds)
