import html

import pandas as pd
import xml.etree.ElementTree as etree


def convert2int32(code):
    tempcode = code
    lencode = len(code)
    if lencode == 0:
        tempcode = 0
    elif lencode <= 11:
        tempcode = int(code, 10)
        if tempcode > 98000000000:
            tempcode -= 98000000000
    elif len(code) == 36:
        tempcode = int(code[0:7], 16)
    else:
        print(len(code), code)

    return tempcode


def create_xml_header():
    header = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<DATAPACKET Version="2.0">
    <METADATA>
        <FIELDS>
            <FIELD attrname="CODE" fieldtype="i4"/>
            <FIELD attrname="PARENT" fieldtype="i4"/>
            <FIELD attrname="NAME" fieldtype="string.uni" WIDTH="100"/>
            <FIELD attrname="CCOUNT" fieldtype="i4"/>
            <FIELD attrname="FULLPATH" fieldtype="string.uni" WIDTH="16382"/>
            <FIELD attrname="EXTRA" fieldtype="fixed" DECIMALS="3" WIDTH="32"/>
        </FIELDS>
        <PARAMS/>
    </METADATA>
    <ROWDATA>
'''
    return header


def create_xml_ending():
    ending = '''
    </ROWDATA>
</DATAPACKET>'''
    return ending

def htmlEntities( string ):
    return ''.join(['&#{0};'.format(ord(char)) for char in string])

def to_xml(df, filename=None, mode='w'):  # todo исправить на мои выходные данные
    def row_to_xml(row):
        CODE = row.CODE
        PARENT = row.PARENT
        NAME = htmlEntities(row.NAME)
        CCOUNT = row.CCOUNT
        FULLPATH = row.FULLPATH
        xml = '    <ROW CODE="{0}" PARENT="{1}" NAME="{2}" CCOUNT="{3}" FULLPATH="{4}"/>\n'.format(CODE, PARENT, NAME, CCOUNT, FULLPATH)
        return xml

    res = ' '.join(df.apply(row_to_xml, axis=1))

    if filename is None:
        return res
    with open(filename, mode) as f:
        f.write(res)


df = pd.read_csv("goods.csv", delimiter=";", decimal=",", header=0,
                 converters={u'Код': convert2int32, u'Код группы': convert2int32},
                 usecols=[u'Тип', u'Код', u'Код группы', u'Наименование', u'Ед.изм', u'Вес ед. изм', u'Цена',
                          u'Категория'])
# dtypess = df.dtypes
# df[u'Код'] = convert2int32(df[u'Код'])
# df[u'Код группы'] = convert2int32(df[u'Код группы'])
# print(dtypess)
# print(df.info(memory_usage='deep'))

# ищем дубли
# print(df[df.duplicated(keep=False, subset=u'Код') == True].sort_values(by='Код'))  # todo разобраться с дублями!!!
# print (df[u'Код'].value_counts().reset_index().query("Код > 1").rename(columns={'index':'Код', 'Код':'count'}))
# print(df[df[u'Код'] == 1000007376].values)
# закончили искать дубли

# print(sl)
dfGroups = df[df[u'Тип'] == u"Группа"].fillna(0)  # это выборка групп
dfItems = df[df[u'Тип'] != u"Группа"]  # это выборка товаров
#print(dfGroups)

# РАБОТАЕМ С ГРУППАМИ
# todo надо добавить столбцы "CCOUNT" и "FULLPATH"
dfGroups.drop(columns=['Тип', 'Ед.изм', 'Вес ед. изм', 'Цена', 'Категория'], inplace=True)
dfGroups.rename(index=str, columns={"Код": "CODE","Код группы":"PARENT","Наименование":"NAME"}, inplace=True)
dfGroups.sort_values(by=['PARENT', 'CODE'],inplace=True)
# print(dfGroups.loc[:,['CODE','PARENT']])

dfGroups.set_index('CODE', inplace=True)
# print(dfGroups)
dfGroupsChildCount=dfGroups.groupby("PARENT")["PARENT"].count().rename("CCOUNT").reset_index()\
    .rename(index=str, columns = {'PARENT':'CODE'}).set_index('CODE')
# print(dfGroupsChildCount)
# dfGroups['count'] = dfGroups.groupby("Код группы")["Код группы"].transform(len)
# print(dfGroups)
dfGroupsWithCCOUNT = dfGroups.join(dfGroupsChildCount).fillna(0)
dfGroupsWithCCOUNT["CCOUNT"] = dfGroupsWithCCOUNT["CCOUNT"].astype(int)
print(dfGroupsWithCCOUNT)

dfGroupsWithCCOUNT["FULLPATH"] = ""

def getParentPath(index):
    path = str(index) + "."
    ParentIndex = dfGroupsWithCCOUNT.loc[index, ['PARENT']]["PARENT"]
    if ParentIndex == 0:
        return path
    else:
        ppath = getParentPath(ParentIndex)
    return ppath + path


# for i in range(len(dfGroupsWithCCOUNT)):
#     print(dfGroupsWithCCOUNT[i].index.astype(str)+'.')

for index, row in dfGroupsWithCCOUNT.iterrows():
    path = getParentPath(index)
    dfGroupsWithCCOUNT.at[index, 'FULLPATH'] = path
    # print(path)


dfGroupsWithCCOUNT.reset_index( inplace=True)
pass
#tempDF = dfGroupsWithCCOUNT.loc[:,["FULLPATH"]].apply(lambda x:  )
#print(tempDF)

#dfGroups['Код'].
#dfGroups.loc['Код']
#print(dfGroups)
#print(dfGroups.loc[:,('Код','Код группы','count')])
print(create_xml_header()+ to_xml(dfGroupsWithCCOUNT)+create_xml_ending())
with open("SHGROUPS.XML", 'w') as f:
    f.write(create_xml_header()+ to_xml(dfGroupsWithCCOUNT)+create_xml_ending())
# input()
