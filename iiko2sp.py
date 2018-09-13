# import html

import pandas as pd
# import xml.etree.ElementTree as etree


def convert2int32(code):
    tempcode = code
    lencode = len(code)
    if lencode == 0:
        tempcode = 0
    elif lencode <= 11:
        tempcode = int(code, 10)
        if tempcode > 99000000000:
            tempcode -= 99000000000
    elif len(code) == 36:
        tempcode = int(code[32:], 16)
        if tempcode > (65536 / 2):
            tempcode = tempcode - (65536 / 2)
    else:
        print(len(code), code)
    return int(tempcode)


def htmlEntities(string):
    return ''.join(['&#{0};'.format(ord(char)) for char in string])


def readIikoCsv():
    df = pd.read_csv("./DATA/goods.csv", delimiter=";", decimal=",", header=0, low_memory=False,
                     converters={u'Код группы': convert2int32},
                     usecols=[u'Тип', u'Код', u'Код группы', u'Наименование', u'Ед.изм', u'Вес ед. изм', u'Цена',
                              u'Категория'])
    # dtypess = df.dtypes
    # df[u'Код'] = convert2int32(df[u'Код'])
    # df[u'Код группы'] = convert2int32(df[u'Код группы'])
    # print(dtypess)
    # print(df.info(memory_usage='deep'))

    # print(sl)
    dfGroups = df[df[u'Тип'] == u"Группа"].fillna(0)  # это выборка групп
    dfGroups["Код"] = dfGroups["Код"].apply(convert2int32)
    dfItems = df[df[u'Тип'] != u"Группа"]  # это выборка товаров
    return dfGroups, dfItems


def workWithGroups(dfGroups):
    def SHGROUPS_xml_header():
        header = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' \
            '<DATAPACKET Version="2.0">\n    <METADATA>\n        <FIELDS>\n' \
            '            <FIELD attrname="CODE" fieldtype="i4"/>\n' \
            '            <FIELD attrname="PARENT" fieldtype="i4"/>\n' \
            '            <FIELD attrname="NAME" fieldtype="string.uni" WIDTH="100"/>\n' \
            '            <FIELD attrname="CCOUNT" fieldtype="i4"/> \n' \
            '            <FIELD attrname="FULLPATH" fieldtype="string.uni" WIDTH="16382"/>\n' \
            '            <FIELD attrname="EXTRA" fieldtype="fixed" DECIMALS="3" WIDTH="32"/>\n' \
            '        </FIELDS>\n        <PARAMS/>\n    </METADATA>\n    <ROWDATA>\n'
        return header

    def SHGROUPS_xml_ending():
        ending = '    </ROWDATA>\n</DATAPACKET>'
        return ending

    def SHGROUPS_row_to_xml(df):
        def row_to_xml(row):
            CODE = row.CODE
            PARENT = row.PARENT
            NAME = htmlEntities(row.NAME)
            CCOUNT = row.CCOUNT
            FULLPATH = row.FULLPATH
            xml = '        <ROW CODE="{0}" PARENT="{1}" NAME="{2}" CCOUNT="{3}" FULLPATH="{4}"/>\n'.format(CODE, PARENT, NAME, CCOUNT, FULLPATH)
            return xml

        res = ''.join(df.apply(row_to_xml, axis=1))
        return res
    # РАБОТАЕМ С ГРУППАМИ
    # посмотрим на дубли кодов
    duplicates = dfGroups[dfGroups.duplicated(keep=False, subset=u'Код') == True].sort_values(by=u'Код')
    if len(duplicates) is not 0:
        print("Посмотрим на дубли:")
        print(duplicates)
        print("...дубли закончились. Выход.")
        exit
    # /
    dfGroups.drop(columns=['Тип', 'Ед.изм', 'Вес ед. изм', 'Цена', 'Категория'], inplace=True)
    dfGroups.rename(index=str, columns={"Код": "CODE", "Код группы": "PARENT", "Наименование": "NAME"}, inplace=True)

    # print(dfGroups.loc[:,['CODE','PARENT']])

    dfGroups.set_index('CODE', inplace=True)
    # print(dfGroups)
    dfGroupsChildCount = dfGroups.groupby("PARENT")["PARENT"].count().rename("CCOUNT").reset_index()\
        .rename(index=str, columns={'PARENT': 'CODE'}).set_index('CODE')
    # print(dfGroupsChildCount)
    # dfGroups['count'] = dfGroups.groupby("Код группы")["Код группы"].transform(len)
    # print(dfGroups)
    dfGroupsWithCCOUNT = dfGroups.join(dfGroupsChildCount).fillna(0)
    dfGroupsWithCCOUNT["CCOUNT"] = dfGroupsWithCCOUNT["CCOUNT"].astype(int)
    # print(dfGroupsWithCCOUNT)

    # заполняем FULLPATH
    dfGroupsWithCCOUNT["FULLPATH"] = ""

    # for i in range(len(dfGroupsWithCCOUNT)):
    #     print(dfGroupsWithCCOUNT[i].index.astype(str)+'.')

    def getParentPath(index):  # а здесь у нас рекурсия ;)
        path = str(index) + "."
        ParentIndex = dfGroupsWithCCOUNT.loc[index, ['PARENT']]["PARENT"]
        if ParentIndex == 0:
            return path
        else:
            ppath = getParentPath(ParentIndex)
        return ppath + path

    for index, row in dfGroupsWithCCOUNT.iterrows():
        path = getParentPath(index)
        dfGroupsWithCCOUNT.at[index, 'FULLPATH'] = path
        # print(path)

    dfGroupsWithCCOUNT.reset_index(inplace=True)
    # tempDF = dfGroupsWithCCOUNT.loc[:,["FULLPATH"]].apply(lambda x:  )
    # print(tempDF)
    # /

    # сортируем
    # print(dfGroupsWithCCOUNT.sort_values(by=['FULLPATH']))
    dfGroupsWithCCOUNT.sort_values(by=['FULLPATH'], inplace=True)
    # /

    # dfGroups['Код'].
    # dfGroups.loc['Код']
    # print(dfGroups)
    # print(dfGroups.loc[:,('Код','Код группы','count')])
    # print(SHGROUPS_xml_header() + SHGROUPS_row_to_xml(dfGroupsWithCCOUNT) + SHGROUPS_xml_ending())
    return SHGROUPS_xml_header() + SHGROUPS_row_to_xml(dfGroupsWithCCOUNT) + SHGROUPS_xml_ending()
    # /РАБОТАЕМ С ГРУППАМИ


def workWithItems(dfItems):
    def GOODS_xml_header():
        header = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' \
            '<DATAPACKET Version="2.0">\n    <METADATA>\n        <FIELDS>\n' \
            '''            <FIELD attrname="KIND" fieldtype="i4"/>
            <FIELD attrname="CODE" fieldtype="string.uni" WIDTH="40"/>
            <FIELD attrname="NAME" fieldtype="string.uni" WIDTH="200"/>
            <FIELD attrname="ABBREV" fieldtype="string.uni" WIDTH="34"/>
            <FIELD attrname="EDIZM" fieldtype="string.uni" WIDTH="10"/>
            <FIELD attrname="SECTION" fieldtype="i4"/>
            <FIELD attrname="RETSECTION" fieldtype="i4"/>
            <FIELD attrname="PRICE" fieldtype="fixed" DECIMALS="2" WIDTH="32"/>
            <FIELD attrname="GROUP" fieldtype="i2"/>
            <FIELD attrname="TAXINDEX" fieldtype="i2"/>
            <FIELD attrname="NOTE" fieldtype="string.uni" WIDTH="200"/>
            <FIELD attrname="NDS" fieldtype="fixed" DECIMALS="2" WIDTH="32"/>
            <FIELD attrname="FIRM_ID" fieldtype="i4"/>
            <FIELD attrname="SPECIAL_PRICE" fieldtype="boolean"/>
            <FIELD attrname="COMPLEX" fieldtype="boolean"/>
            <FIELD attrname="DISMISS" fieldtype="boolean"/>
            <FIELD attrname="TAXATION" fieldtype="i2"/>''' \
            '\n        </FIELDS>\n        <PARAMS/>\n    </METADATA>\n    <ROWDATA>\n'
        return header

    def GOODS_xml_ending():
        ending = '    </ROWDATA>\n</DATAPACKET>'
        return ending

    def GOODS_row_to_xml(df):
        def row_to_xml(row):
            # KIND = row.KIND
            # CODE = row.CODE
            # NAME = htmlEntities(row.NAME)
            # EDIZM = row.EDIZM
            # SECTION = row.SECTION
            KIND = row.KIND
            CODE = row.CODE
            NAME = htmlEntities(row.NAME)
            EDIZM = htmlEntities(row.EDIZM)
            SECTION = row.SECTION
            # RETSECTION = row.RETSECTION
            PRICE = row.PRICE
            GROUP = row.GROUP
            TAXINDEX = row.TAXINDEX
            SPECIAL_PRICE = row.SPECIAL_PRICE
            COMPLEX = row.COMPLEX
            DISMISS = row.DISMISS
            # TAXATION = row.TAXATION
            # xml = f'    <ROW KIND="{KIND}" CODE="{CODE}" NAME="{NAME}" EDIZM="{EDIZM}" SECTION="{SECTION}"\
            #     RETSECTION="{RETSECTION}" PRICE="{PRICE}" GROUP="{GROUP}" TAXINDEX="{TAXINDEX}"\
            #     SPECIAL_PRICE="{SPECIAL_PRICE}" COMPLEX="{COMPLEX}" DISMISS="{DISMISS}" TAXATION="{TAXATION}"\
            #     />\n'
            xml = f'    <ROW KIND="{KIND}" CODE="{CODE}" NAME="{NAME}" EDIZM="{EDIZM}" SECTION="{SECTION}"\
 PRICE="{PRICE}" GROUP="{GROUP}" TAXINDEX="{TAXINDEX}"\
 SPECIAL_PRICE="{SPECIAL_PRICE}" COMPLEX="{COMPLEX}" DISMISS="{DISMISS}"/>\n'
            return xml

        res = ''.join(df.apply(row_to_xml, axis=1))
        return res

    # dfItems.drop(columns=['Тип', 'Ед.изм', 'Вес ед. изм', 'Цена', 'Категория'], inplace=True)
    dfItems["KIND"] = "0"
    dfItems.rename(index=str, columns={
        "Код": "CODE",
        "Наименование": "NAME",
        "Ед.изм": "EDIZM",

        "Код группы": "GROUP"}, inplace=True)

    dfItems["SECTION"] = "0"
    dfItems["RETSECTION"] = "0"
    dfItems["PRICE"] = "0.00"

    dfItems["TAXINDEX"] = "0"  # не нужен
    dfItems["SPECIAL_PRICE"] = "FALSE"  # не нужен
    dfItems["COMPLEX"] = "FALSE"  # ЭТО СОСТАВНОЙ ТОВАР - у него нет остатков!!! и списываются составные части
    dfItems["DISMISS"] = "FALSE"  # списывать при возврате! - типа кофе (составной товар) чтоли?
    # dfItems["TAXATION"] = "0"  # для составного товара "8"???
    duplicates = dfItems[dfItems.duplicated(keep=False, subset=u'CODE') == True].sort_values(by=u'CODE')
    if len(duplicates) is not 0:
        print("Посмотрим на дубли GOODS:")
        print(duplicates)
        print("...дубли закончились. Выход.")
        exit
    return GOODS_xml_header() + GOODS_row_to_xml(dfItems) + GOODS_xml_ending()


def main():
    dfGroups, dfItems = readIikoCsv()

    SHGROUPS_xml = workWithGroups(dfGroups)

    with open("./DATA/SHGROUPS_.XML", 'w') as f:
        f.write(SHGROUPS_xml)

    GOODS_xml = workWithItems(dfItems)

    with open("./DATA/GOODS_.XML", 'w') as f:
        f.write(GOODS_xml)
    # print(dfGroups)


if __name__ == "__main__":
    main()
