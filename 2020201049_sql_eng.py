#!/usr/bin/python3
import itertools
import sqlparse
import re
import csv
import sys

from sqlparse import sql,tokens as T
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Whitespace,Wildcard

tables = {}

tablenames = []

col_to_table = {}

def readmeta():
    f = open("metadata.txt","r")
    lines = f.readlines()
    tablestart = False
    tablename = None
    cnt = 0

    for line in lines:
        line = line.strip()
    
        if(line=='<end_table>'):
            tablestart = False
            tablename = None
            cnt = 0
            continue
        if(line=='<begin_table>'):
            tablestart = True
            continue
        if(tablestart==True):
            tablename = line
            tablenames.append(tablename.lower())
            tables[tablename] = [[[]],{}]
            tablestart = False
            continue
        col_to_table[line.lower()] = tablename
        tables[tablename][1][line.lower()] = cnt
        cnt += 1
    f.close()


def readtables():
    for table in tables:
        f = open("{}.csv".format(table))
        data = list(csv.reader(f))
        tables[table][0] = data
        f.close()


def join(tables, tablenames):
    if(len(tablenames)<2):
        joinedcols = ['']*len(tables[tablenames[0]][1])
        for col in tables[tablenames[0]][1]:
            joinedcols[tables[tablenames[0]][1][col]] = col
        return tables[tablenames[0]],joinedcols
    
    joined = join_helper(tables[tablenames[0]], tables[tablenames[1]])

    for i in range(2, len(tablenames)):
        joined = join_helper(joined, tables[tablenames[i]])

    joinedcols = ['']*len(joined[1])

    for col in joined[1]:
        joinedcols[joined[1][col]] = col
    return joined,joinedcols
     
    
def join_helper(table1,table2):
    t1data = table1[1]
    t2data = table2[1]
    table = [[],{}]
    for i in range(0,len(table1[0])):
        for j in range(0,len(table2[0])):
            table[0].append(table1[0][i] + table2[0][j])
    t2data_n = {}
    n = len(t1data)
    for col in t2data:
        t2data_n[col] = t2data[col] + n
    tdata = {**t1data, **t2data_n}
    table[1] = tdata
    return table


def where(table, whereconds, whereop):
    if(len(whereconds)==0):
        return table
    data = table[0]
    meta = table[1]

    result = []

    for i in range(0,len(data)):
        if(len(whereop)>0):
            flag = True if whereop[0].lower()=='and' else False
        else:
            flag = True
        for cond in whereconds:
            v1 = None
            v2 = None

            if(cond[0] in table[1]):
                ind = table[1][cond[0]]
                v1 = int(data[i][ind])  
            else:
                try:
                    v1 = int(cond[0])
                except:
                    print("one")
                    print("Invalid Column")
                    exit(0)
            if(cond[2] in table[1]):
                ind = table[1][cond[2]]
                v2 = int(data[i][ind])
            else:
                try:
                    v2 = int(cond[2])
                except:
                    print("two")
                    print("Invalid Column")

            if(cond[1]=='='):
                cond[1]='=='

            if(len(whereop)>0):
                condstr = "{}".format(v1) + cond[1] + "{}".format(v2)
                evalres = eval(condstr)
                flag = eval("flag {}".format(whereop[0]).lower() + " " + str(evalres))
            else:
                condstr = "{}".format(v1) + cond[1] + "{}".format(v2)
                flag = eval(condstr)
        
        if(flag):
            result.append(data[i])
    table[0] = result
    return table

def agg_max(data,col):
    return str(max(int(row[col]) for row in data))
    
def agg_min(data,col):
    return str(min(int(row[col]) for row in data))

def agg_sum(data,col):
    return str(sum(int(row[col]) for row in data))

def agg_avg(data,col):
    return str(sum(int(row[col]) for row in data)/len(data))


def agg_count(data,col,distinct):
    if(col=='*' or distinct==False):
        return str(len(data))
    s = set()
    for row in data:
        s.add(data[col])
    return str(len(s))


def project(table, cols):

    if(starflag==True):
        revmap = ['']*len(table[1])
        for col in table[1]:
            revmap[table[1][col]] = col

        for col in revmap:
            print( col_to_table[col] + "." + col, end='\t')
        print()

        if(distinctflag==True):
            s = set()
            for row in table[0]:
                if(tuple(row) in s):
                    continue
                s.add(tuple(row))
                for i in range(0,len(row)):
                    print(row[i],end='\t')
                print()
            return

        for row in table[0]:
            for i in range(0,len(row)):
                print(row[i],end='\t')
            print()
        return
    for col in cols:
        if(col not in col_to_table):
            print("three")
            print("Invalid Column.")
            exit(0)
        print(col_to_table[col] +"." + col,end='\t')
    print()

    if(distinctflag==True):
        s = set()
        for row in table[0]:
            currow = []
            for col in cols:
                if(col not in table[1]):
                    print("four")
                    print("Invalid Column.")
                    exit(0)
                currow.append(row[table[1][col]])
            if(tuple(currow) in s):
                continue
            for ele in currow:
                print(ele,end='\t')
            s.add(tuple(currow))
            print()
        return

    for row in table[0]:
        for col in cols:
            if(col not in table[1]):
                print("five")
                print("Invalid Column.")
                exit(0)
            print(row[table[1][col]],end='\t')
        print()

def execagg(data, agg, meta):
    row = []
    # print(meta)
    for fun in agg:
        # print(fun)
        # print(fun[1])
        if(fun[1] not in meta):
            print("Invalid Aggregate Col.")
            exit(0)
        val = None
        distinct = False
        if(fun[0].lower()=='max'):
            # group = list(grp)
            val = agg_max(data,meta[fun[1]])
        elif(fun[0].lower()=='min'):
            val = agg_min(data,meta[fun[1]])
        elif(fun[0].lower()=='sum'):
            val = agg_sum(data,meta[fun[1]])
        elif(fun[0].lower()=='average' or fun[0].lower()=='avg'):
            val = agg_avg(data,meta[fun[1]])
        elif(fun[0].lower()=='count'):
            ccol = fun[1].split(' ')
            for i in range(0,len(ccol)):
                ccol[i] = ccol[i].lower()
            if('distinct' in ccol):
                distinct = True
            val = agg_count(data,meta[fun[1]],distinct)
        row.append(val)
    return row

def groupby(table, gbcol,qcols,agg):

    if(gbcol is None):
        ##########if no group by present then either all cols have aggregate or none have aggregate.

        if(len(agg)==0):
            #project qcols.
            if(isorderby==True):
                if(obcol not in table[1]):
                    print("Invalid Order By Col.")
                    exit(0)
                ind = table[1][obcol]
                flag = False if order=='asc' else True
                data = sorted(table[0], key=lambda x : int(x[ind]), reverse=flag)
                table[0] = data
            project(table,qcols)
        elif(len(qcols)==0):
            for fun in agg:
                if(fun[1] not in col_to_table):
                    print("Invalid Column in aggregate.")
                    exit(0)
                print(col_to_table[fun[1]] + "."+  fun[0] + "(" + fun[1] + ")", end='\t')
            print()
            result = execagg(table[0], agg, table[1])
            for ele in result:
                print(ele,end='\t\t')
            print()
        else:
            print("group by error.")
        return
    if(len(qcols)>1):
        print("Col without aggregate function present in select which is not present in group by")
        exit(0)

    data = table[0]
    meta = table[1]

    if(gbcol not in table[1]):
        print("Invalid Group by Column.")
        exit(0)

    ind = table[1][gbcol]

    result = []


    data = sorted(data, key=lambda x : int(x[ind]))
    grouped = itertools.groupby(data, lambda x : int(x[ind]))
    for k,grp in grouped:
        row = []
        row.append(k)
        group = list(grp)
        aggres = execagg(group, agg,table[1])
        result.append(row+aggres)

    if(isorderby==True):
        if(order=='desc'):
            result = sorted(result, key = lambda x : int(x[0]), reverse=True)


    print(col_to_table[gbcol] + "." + gbcol,end='\t')

    for fun in agg:
        print(col_to_table[fun[1]] + "."+  fun[0] + "(" + fun[1] + ")", end='\t')
    print()
    for row in result:
        for col in row:
            print(col, end='\t\t')
        print()
    return result 


readmeta()
readtables()
# query = 'select distinct act_gender from actor order by act_gender'
# query = 'select act_id_a, sum(mov_id_am), count(earnings) from actor, actor_movie, movie group by act_id_a order by act_id_a desc'
# query = 'select act_id_a, sum(mov_id_am), count(earnings) from actor,actor_movie,movie where act_gender=0 or act_id_a>110 group by act_id_a order by act_id_a' #CHECK
# query = 'select a,count(b) from table1 where group by a'
# query = 'select distinct c,d,e,f from table2'
# query = 'select distinct a, sum(b), avg(c), min(d) from table2' #error
# query = 'select distinct c, sum(d), avg(e), min(f) from table2 group by c' 
# query = 'select max(mov_id_m), min(mov_time) from movie group by mov_year order by mov_year desc' 
# query = 'select  min(mov_time),max(mov_id_m) from movie group by mov_year order by mov_year desc' 
# query = 'select earnings, mov_year from movie order by earnings asc' 
# query = 'select max(mov_id_m), min(mov_time) from movie group by earnings order by earnings desc' 

# query = 'select max(mov_id_m), min(mov_time) from movie'

# query = 'select * from movie,actor'
# query  = 'select * from movie where mov_id_m = 901'


try:
    query = sys.argv[1].lower()
except:
    print("Query not provided")
    exit(0)
if(query[-1]!=';'):
    print("Missing Semicolon at the end of the query.")
    exit(0)
if('select' not in query or 'from' not in query):
    print("Invalid Query")
    exit(0)
parsed = sqlparse.parse(query)[0]



tokens = parsed.tokens

selflag=False
isgroupby = False
pgroupby = False

isorderby = False
porderby = False

whereflag = False
distinctflag = False
qtables = []
qcols = []
agg = []
gbcol = None
obcol = None
order = None 

whereconds = []
whereop = []

starflag = False

for item in tokens:
    if(item.ttype is Whitespace):
        continue
 
    if item.ttype is DML and item.value.upper() == 'SELECT':
        selflag = True
        continue

    if item.ttype is Keyword and item.value.upper() == 'GROUP BY':
        isgroupby = True
        pgroupby = True
        continue
    if item.ttype is Keyword and item.value.upper() == 'ORDER BY':
        isorderby = True
        porderby = True
        continue
    if(isinstance(item,Where)):
        for subitem in item.tokens:
            if(isinstance(subitem,Comparison)):
                wherecond = []
                for subsubitem in subitem.tokens:
                    if(isinstance(subsubitem, Identifier)):
                        wherecond.append(subsubitem.value.lower())
                    elif(subsubitem.ttype==T.Number.Integer):
                        wherecond.append(subsubitem.value)
                    elif(subsubitem.ttype==T.Comparison):
                        wherecond.append(subsubitem.value)
                whereconds.append(wherecond)
            elif(subitem.ttype is Keyword and subitem.value.lower()=='and' or subitem.value.lower()=='or'):
                whereop.append(subitem.value)
        continue

    elif(pgroupby==True):
        if(isinstance(item,Identifier)):
            gbcol = item.value.replace('"', '').lower()
        else:
            print("Group ByError.")
            exit(0)
        pgroupby = False
        continue
    elif(porderby==True):    
        if(isinstance(item,Identifier)):
            obcol = item.value.replace('"', '').lower()
            order = 'asc'
            tmp = obcol.split(' ')
            if(len(tmp)>1):
                order = tmp[1]
                obcol = tmp[0].lower()
        else:
            print("OrderBy Error.")
            exit(0)
        porderby = False
        continue
        

    if(selflag==True):

        if(item.ttype is Keyword and item.value.upper()=='DISTINCT'):
            distinctflag = True
            continue

        if(item.ttype is Wildcard):
            starflag = True
            selflag = False
            continue
        if(isinstance(item,IdentifierList)):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                m = re.match(r"^\s*(\w+)\s*\((.*)\)",value)
                if(m is not None):
                    agg.append([m.group(1).lower(), m.group(2).lower()])
                else:
                    qcols.append(value.lower())
            selflag = False

        elif(isinstance(item, Identifier)):
            value = item.value.replace('"', '').lower()
            m = re.match(r"^\s*(\w+)\s*\((.*)\)",value)
            if(m is not None):
                agg.append([m.group(1).lower(), m.group(2).lower()])
            else:
                value = item.value.replace('"', '').lower()
                qcols.append(value.lower())
            selflag = False

        elif(isinstance(item,Function)):
            # print("here")
            value = item.value.lower()
            m = re.match(r"^\s*(\w+)\s*\((.*)\)",value)
            # print(m)
            agg.append([m.group(1).lower(), m.group(2).lower()])
            selflag = False
    else:
        if(isinstance(item, IdentifierList)):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                if(value not in tablenames):
                    print("Invalid tablename")
                    exit(0)
                qtables.append(value.lower())
            
        if(isinstance(item,Identifier)):
            value = item.value.replace('"', '').lower()
            if(value not in tablenames):
                print("Invalid tablename")
                exit(0)
            qtables.append(value.lower())
# print(tokens)
# print(agg)
joined_table,joined_cols = join(tables,qtables)
joined_table = where(joined_table,whereconds,whereop)
joined_table = groupby(joined_table, gbcol,qcols,agg)

