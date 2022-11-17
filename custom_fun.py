import re,requests
from pack.xiedaxia import text_date
import pandas as pd

def two_ls(re,text):
    # 两个的
    ls = re.findall(text)
    start_ls = []
    end_ls = []
    for i in range(len(ls)):
        if i % 2 == 0:
            start_ls.append(ls[i])
        elif i % 2 == 1:
            end_ls.append(ls[i])
        else:
            raise KeyError("函数：two_ls ,出错")
    return start_ls,end_ls

def three_ls(re,text):
    # 三个的
    ls = re.findall(text)
    color_ls = []
    in_state_ls = []
    box_state_ls = []
    for i in range(len(ls)):
        if i % 3 == 0:
            color_ls.append(ls[i])
        elif i % 3 == 1:
            in_state_ls.append(ls[i])
        elif i % 3 == 2:
            box_state_ls.append(ls[i])
        else:
            raise KeyError("函数：three_ls ,出错")
    return color_ls,in_state_ls,box_state_ls

def four_ls(re,text):
    # 四个的
    ls = re.findall(text)
    ls1 = []
    ls2 = []
    ls3 = []
    ls4 = []
    for i in range(len(ls)):
        if i % 4 == 0:
            ls1.append(ls[i])
        elif i % 4 == 1:
            ls2.append(ls[i])
        elif i % 4 == 2:
            ls3.append(ls[i])
        elif i % 4 == 3:
            ls4.append(ls[i])
        else:
            raise KeyError("函数：four_ls ,出错")
    return ls1,ls2,ls3,ls4

def data_process(text):
    re_1 = re.compile("<tr.*/tr>")
    text = re_1.findall(text)[0].replace("<span class=\'ckbox\'/>", "").replace("\\r", "").replace("\\", "")
    html = f"<table>{text}</table>"
    return html

def data_process_old(text): # 出库是另外的
    re_1 = re.compile('r(TF\d+)')  # 单据编号
    re_2 = re.compile('\\\\\\\\r(2022\d{4})\\\\\\\\r')  # 单据日期，入库日期
    re_3 = re.compile('(P[A-Z]{8}[0-9]{3})\\\\\\\\r')  # 商品
    re_4 = re.compile('(P[A-Z]{8}[0-9]{7})\\\\\\\\r')  # 条码
    re_5 = re.compile(
        'width=\\\\\\\\\\\\"17%\\\\\\\\\\\\" >\\\\\\\\r([A-Z]?[\u4e00-\u9fa5]+T?[\u4e00-\u9fa5]?)\\\\\\\\r')  # 品名
    re_6 = re.compile('width=\\\\\\\\\\\\"0%\\\\\\\\\\\\" >\\\\\\\\r([\u4e00-\u9fa5]+)\\\\\\\\r')  # 颜色，入库状态，装箱状态
    re_7 = re.compile('width=\\\\\\\\\\\\"7%\\\\\\\\\\\\" >\\\\\\\\r(\d{2})\\\\\\\\r')  # 尺码
    re_8 = re.compile('width=\\\\\\\\\\\\"0%\\\\\\\\\\\\" >\\\\\\\\r(-?\d{0,6})\\\\\\\\r')  # 出库数量 ,入库数量
    re_9 = re.compile('width=\\\\\\\\\\\\"1%\\\\\\\\\\\\" >\\\\\\\\r(-?\d{1,8}.\d{0,5})\\\\\\\\r')  # 标准价,标准金额,入库标准金额
    re_10 = re.compile(
        '<img border=\\\\\'0\\\\\' src=\\\\\'/html/nds/images/out.png\\\\\'/><\\\\\\\\/a>&nbsp;([\u4e00-\u9fa5]+[0-9]{0,9}[A-Z]{0,9}[a-z]{0,9}[\u4e00-\u9fa5]+[0-9]{0,9}|[a-z]+)\\\\\\\\r')  # 收货方,入库人,修改人,出库人
    re_11 = re.compile(
        '<td nowrap align=\\\\\\\\\\\\"left\\\\\\\\\\\\" width=\\\\\\\\\\\\"1%\\\\\\\\\\\\" >\\\\\\\\r(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\\\\\\\\r')  # 入库时间，修改时间，出库时间
    re_12 = re.compile('width=\\\\\\\\\\\\"5%\\\\\\\\\\\\" >\\\\\\\\r([\u4e00-\u9fa5]+)\\\\\\\\r')  # 单据类型,发货方
    order_id_ls = re_1.findall(text)  # 单据编号
    order_date_ls, in_date_ls = two_ls(re_2, text)  # 单据日期，入库日期
    sku_id_ls = re_3.findall(text)  # 商品
    sku_code_ls = re_4.findall(text)  # 条码
    category_ls = re_5.findall(text)  # 品名
    color_ls, in_state_ls, box_state_ls = three_ls(re_6, text)  # 颜色，入库状态，装箱状态
    size_ls = re_7.findall(text)  # 尺码
    out_quantity_ls, in_quantity_ls = two_ls(re_8, text)  # 出库数量 ,入库数量
    price_ls, amount_ls, in_amount_ls = three_ls(re_9, text)  # 标准价,标准金额,入库标准金额
    consignee_ls, in_warehousing_ls, modifiying_ls, out_warehousing_ls = four_ls(re_10, text)  # 收货方,入库人,修改人,出库人
    inbound_time_ls, modifiying_time_ls, outbound_time_ls = three_ls(re_11, text)  # 入库时间，修改时间，出库时间
    order_type_ls, shipper_ls = two_ls(re_12, text)  # 单据类型，发货方

    title_ls = ['单据编号', '单据类型', '单据日期', '发货方', '收货方', '入库日期', '商品', '条码', '品名', '颜色', '尺寸', '出库数量', '入库数量', '标准价',
                '标准金额', '入库标准金额', '入库状态', '入库人', '入库时间', '装箱状态', '修改人', '修改时间', '出库人', '出库时间', '可用']
    d = {
        "单据编号": order_id_ls,
        "单据日期": order_date_ls,
        "入库日期": in_date_ls,
        "商品": sku_id_ls,
        "条码": sku_code_ls,
        "品名": category_ls,
        "颜色": color_ls,
        "入库状态": in_state_ls,
        "装箱状态": box_state_ls,
        "尺寸": size_ls,
        "出库数量": out_quantity_ls,
        "入库数量": in_quantity_ls,
        "标准价": price_ls,
        "标准金额": amount_ls,
        "入库标准金额": in_amount_ls,
        "收货方": consignee_ls,
        "入库人": in_warehousing_ls,
        "修改人": modifiying_ls,
        "出库人": out_warehousing_ls,
        "入库时间": inbound_time_ls,
        "修改时间": modifiying_time_ls,
        "出库时间": outbound_time_ls,
        "单据类型": order_type_ls,
        "发货方":shipper_ls,
        "可用": ["是"] * len(order_id_ls)
    }
    df = pd.DataFrame(d)
    df["单据日期"] = df["单据日期"].apply(text_date)
    df["入库日期"] = df["入库日期"].apply(text_date)
    return df[title_ls]

def data_scraping(url,cookies,headers,data):
    re_u = re.compile(r"\\u[0-9]?[A-Z]?[0-9]?[A-Z]?[0-9]?[A-Z]?[0-9]?[A-Z]?[0-9]?[A-Z]?")  # 提取Unicode
    response = requests.post(url=url, cookies=cookies, headers=headers, data=data)
    text = response.text
    for i in set(re_u.findall(text)):
        c = i.encode().decode("unicode_escape")
        text = text.replace(i, c)
    return text

def data_process_1881(text):
    re_1 = re.compile('TF\d+')  # 单据编号
    re_2 = re.compile('\\\\\\\\r(2022\d{4})\\\\\\\\r')  # 单据日期，入库日期
    re_3 = re.compile('(H[A-Z][0-9]{0,2}[A-Z]{4}[0-9]{3})\\\\\\\\r')  # 商品
    re_4 = re.compile('(H[A-Z][0-9]{0,2}[A-Z]{4}[0-9]{7})\\\\\\\\r')  # 条码
    re_5 = re.compile(
        'width=\\\\\\\\\\\\"1[4,7]%\\\\\\\\\\\\" >\\\\\\\\r([A-Z]?[\u4e00-\u9fa5]+T?[\u4e00-\u9fa5]?)\\\\\\\\r')  # 品名
    re_6 = re.compile('width=\\\\\\\\\\\\"0%\\\\\\\\\\\\" >\\\\\\\\r([\u4e00-\u9fa5]+)\\\\\\\\r')  # 颜色，入库状态，装箱状态
    re_7 = re.compile('width=\\\\\\\\\\\\"[5,7]%\\\\\\\\\\\\" >\\\\\\\\r(\d{2})\\\\\\\\r')  # 尺码
    re_8 = re.compile('width=\\\\\\\\\\\\"0%\\\\\\\\\\\\" >\\\\\\\\r(-?\d{0,6})\\\\\\\\r')  # 出库数量 ,入库数量
    re_9 = re.compile('width=\\\\\\\\\\\\"[01]%\\\\\\\\\\\\" >\\\\\\\\r(-?\d{1,8}\.\d+)\\\\\\\\r')  # 标准价,标准金额,入库标准金额
    re_10 = re.compile(
        '<img border=\\\\\'0\\\\\' src=\\\\\'/html/nds/images/out.png\\\\\'/><\\\\\\\\/a>&nbsp;([\u4e00-\u9fa5]{0,9}[0-9]{0,9}[\u4e00-\u9fa5]{0,9}|[a-z]+[0-9]{0,2})\\\\\\\\r')  # 收货方,入库人,修改人,出库人
    re_11 = re.compile(
        '<td nowrap align=\\\\\\\\\\\\"left\\\\\\\\\\\\" width=\\\\\\\\\\\\"1%\\\\\\\\\\\\" >\\\\\\\\r(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\\\\\\\\r')  # 入库时间，修改时间，出库时间
    re_12 = re.compile(
        'width=\\\\\\\\\\\\"[4,5]%\\\\\\\\\\\\" >\\\\\\\\r([0-9]*[\u4e00-\u9fa5]+)\\\\\\\\r')  # 单据类型， 收货方
    order_id_ls = re_1.findall(text)  # 单据编号
    order_date_ls, in_date_ls = two_ls(re_2, text)  # 单据日期，入库日期
    sku_id_ls = re_3.findall(text)  # 商品
    sku_code_ls = re_4.findall(text)  # 条码
    category_ls = re_5.findall(text)  # 品名
    color_ls, in_state_ls, box_state_ls = three_ls(re_6, text)  # 颜色，入库状态，装箱状态
    size_ls = re_7.findall(text)  # 尺码
    out_quantity_ls, in_quantity_ls = two_ls(re_8, text)  # 出库数量 ,入库数量
    price_ls, amount_ls, in_amount_ls = three_ls(re_9, text)  # 标准价,标准金额,入库标准金额
    consignee_ls, in_warehousing_ls, modifiying_ls, out_warehousing_ls = four_ls(re_10, text)  # 收货方,入库人,修改人,出库人
    inbound_time_ls, modifiying_time_ls, outbound_time_ls = three_ls(re_11, text)  # 入库时间，修改时间，出库时间
    order_type_ls, shipper_ls = two_ls(re_12, text)  # 单据类型，发货方

    title_ls = ['单据编号', '单据类型', '单据日期', '发货方', '收货方', '入库日期', '商品', '条码', '品名', '颜色', '尺寸', '出库数量', '入库数量', '标准价',
                '标准金额', '入库标准金额', '入库状态', '入库人', '入库时间', '装箱状态', '修改人', '修改时间', '出库人', '出库时间', '可用']
    d = {
        "单据编号": order_id_ls,
        "单据日期": order_date_ls,
        "入库日期": in_date_ls,
        "商品": sku_id_ls,
        "条码": sku_code_ls,
        "品名": category_ls,
        "颜色": color_ls,
        "入库状态": in_state_ls,
        "装箱状态": box_state_ls,
        "尺寸": size_ls,
        "出库数量": out_quantity_ls,
        "入库数量": in_quantity_ls,
        "标准价": price_ls,
        "标准金额": amount_ls,
        "入库标准金额": in_amount_ls,
        "收货方": consignee_ls,
        "入库人": in_warehousing_ls,
        "修改人": modifiying_ls,
        "出库人": out_warehousing_ls,
        "入库时间": inbound_time_ls,
        "修改时间": modifiying_time_ls,
        "出库时间": outbound_time_ls,
        "单据类型": order_type_ls,
        "发货方":shipper_ls,
        "可用": ["是"] * len(order_id_ls)
    }
    df = pd.DataFrame(d)
    df["单据日期"] = df["单据日期"].apply(text_date)
    df["入库日期"] = df["入库日期"].apply(text_date)
    return df[title_ls]

