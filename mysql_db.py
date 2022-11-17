import pymysql,time
from pack.xiedaxia import save_to_sql,get_file_path
from pymysql.constants import CLIENT
import pandas as pd
import numpy as np


class Mysqldb:
    def __init__(self,host="127.0.0.1",port=3306,user="root",passwd="root",db_name="cartelo"):
        pass
        # 建立数据库链接
        self.conn = pymysql.connect(
            host = host,
            port = port,
            user = user,
            passwd = passwd,
            db = db_name,
            client_flag=CLIENT.MULTI_STATEMENTS # 多个语句发给MySQL
        )
        # 通过 cursor() 创建游标对象， 并让查询结果以字典的格式输出
        self.cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    def __del__(self): # 对象资源被释放时出发， 在对象即将被删除时的最后操作
        # 关闭游标
        self.cur.close()
        # 关闭数据库链接
        self.conn.close()

    def select_db(self,sql):
        """查询数据库"""
        # 使用 execute() 执行 sql
        self.cur.execute(sql)
        # 使用 fetchall（）获取查询结果
        data = self.cur.fetchall()
        return data

    def selects_db(self,sql):
        """多个查询语句"""
        data_ls = []
        for sql_ in sql.split(";"):
            self.cur.execute(sql_)
            data = self.cur.fetchall()
            data_ls += data
        return data_ls

    def execute_db(self,sql):
        """更新/插入/删除"""
        try:
            # 使用 execute() 执行sql
            self.cur.execute(sql)
            # 提交事务
            self.conn.commit()
        except Exception as e:
            print(f"操作出错：{e}")
            # 回滚所有更改
            self.conn.rollback()


def polo_db_link():
    """
    :return:保罗数据库连接
    """
    # 数据库
    db = Mysqldb("localhost", 3306, "root", "root", "cartelo")
    return  db

def show_data(df,condition,calculate):
    "合并计算数据库里面的数据"
    df[calculate] = df[calculate].astype(int)
    date_ls = df.groupby(condition)[calculate].sum().to_dict()
    # for i in date_ls:
        # print(f"{i}:   {date_ls[i]}") # 打印
    return date_ls

def conpare_date(db_d,sale_d,show = False):
    # 对比两组数据，返回其中差异值
    ls = []
    for i in sale_d.items():
        value = db_d.get(i[0],0)
        if show == True:
            print(i[0],end = ":")
            print(i[1],end = "\t-*-\t")
            print(f"旧数据：{value}",end = "\t-*-\t")
            print(f"差额 ：{i[1] - value}")
        if value != i[1]:
            ls.append(i[0])
    return ls

def filter_data(df,ls,condition):
    """
    :param df: 新的DF的
    :param ls: 不同日期的list
    :param condition: df的列名
    :return:
    """
    df_ls = []
    for i in ls:
        df_ = df[df[condition] == i]
        df_ls.append(df_)
    df_new = pd.concat(df_ls)
    return df_new

def create_ins(df,table_name):
   # 创建sql插入代码
    sql = ""
    ls = np.array(df).tolist()
    for i in ls:
        sql_ = f"INSERT INTO `{table_name}` VALUES({str(i)[1:-1]});"
        sql += sql_ + "\n"
    print(sql)
    return sql

def crearte_del(ls,condition,table_name):
    # 创建sql删除代码
    sql = ""
    for i in ls:
        sql_ = f'delete from `{table_name}` WHERE `{condition}` = "{i}";'
        sql += sql_
    return sql

def crearte_select(ls,columns_ls,condition,table_name):
    """
    :param ls: 商品ID
    :param columns_ls: 需要的列名
    :param condition:  条件列
    :param table_name: 表名
    :return:  sql语句
    """
    if columns_ls == "*":
        columns = "*"
    else:
        columns = ",".join([f"`{i}`" for i in columns_ls ]) # 格式化
    # 创建sql删除代码
    sql = ""

    for i in ls:
        sql_ = f'select {columns} from `{table_name}` WHERE `{condition}` = "{i}";'
        sql += sql_
    return sql

def data_compard(db,db_d,new_d,new_df,condition,table_name,db_name):
    # 数据对比并合并
    dif_ls = conpare_date(db_d,new_d,show=True)
    if len(dif_ls) > 0:
        # 筛选数据
        fl_df = filter_data(new_df,dif_ls,condition)
        # 删除不同的数据
        delete_sql = crearte_del(dif_ls,condition,table_name)
        db.execute_db(delete_sql)
        # 保存新数据
        save_to_sql(fl_df,db_name,table_name)
    else:
        print("数据无更新")

def read_sqlData(db,condition,calculate,table_name,start_date):
    # 读取数据库数据
    s = time.time()
    select_sql = f'SELECT `{condition}`,`{calculate}` FROM `{table_name}` WHERE `{condition}` >= "{start_date}"'
    # print(select_sql)
    data = db.select_db(select_sql) # 获取数据库数据
    df = pd.DataFrame(data) # 转成表
    # df[calculate] = df[calculate].apply(float)
    if df.shape[0] != 0:
        df[condition] = pd.to_datetime(df[condition]).astype(str)
        db_d = show_data(df, condition, calculate) # groupby 计算每一个日期的销售数量
        e = time.time()
        print(e-s)
        return db_d
    else:
        print(f"{table_name}没有数据，自行创建")
        df = pd.DataFrame({"出库日期":"","出库数量":""},index = [0])
        return df

def read_newData(path,key_word,condition,calculate):
    # 本地表格数据
    path_ls = get_file_path(path)
    path = [i for i in path_ls  if key_word in i][0]
    df = pd.read_csv(path)
    if df.shape[0] == 0:
        return None,None,None
    df[condition] = df[condition].astype(str)
    d = show_data(df,condition,calculate)
    start_date = min(df[condition])
    return d,df,start_date

if __name__ == '__main__':
    db = Mysqldb("localhost",3306,"root","root","cartelo")

    select_sql = "SELECT * FROM `保罗销售数据` LIMIT 1"
    data = db.select_db(select_sql)
    print(data)