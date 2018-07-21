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


def to_xml(df, filename=None, mode='w'):  # todo исправить на мои выходные данные
    def row_to_xml(row):
        date = row.TIMESTAMP.split()[0]
        time = row.TIMESTAMP.split()[1]
        value = row.A
        xml = '<event date="{0}" time="{1}" value="{2}"></event>'.format(date, time, value)
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
print(df[df.duplicated(keep=False, subset=u'Код') == True])  # todo разобраться с дублями!!!
# print(sl)
dfGroups = df[df[u'Тип'] == u"Группа"]
dfItems = df[df[u'Тип'] != u"Группа"]
# print(dfItems)
# print(create_xml_header(),create_xml_ending())
# input()
