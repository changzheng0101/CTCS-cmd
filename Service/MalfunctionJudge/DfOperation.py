# 通过iloc  可以直接访问到某一行数据
# 截取名字为col_name 的列 并且删除空格
def get_column_data(df, col_name):
    col_name_list = list(df[col_name])
    return list(map(lambda x: str(x).strip(), col_name_list))
