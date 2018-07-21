import pandas as pd
df = pd.read_csv("goods.csv", delimiter=";", decimal=",", header=0,
                 usecols=[u'Тип', u'Код', u'Код группы', u'Наименование', u'Ед.изм', u'Вес ед. изм', u'Цена',
                          u'Категория'], nrows=300)  # в столбце "Код" смешанный тип данных - преобразовать в int32
dtypess = df.dtypes
print(dtypess)
#input()
