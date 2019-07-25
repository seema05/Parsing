import pandas as pd
from itertools import groupby
import re

"""Read the file,groupby "SQL_ID", sortby "PIECE",create lists of queries"""

fileRead = pd.read_csv('D:/sparkProjects/data/TableList.txt', sep="|")
querySort = fileRead.sort_values(['PIECE'], ascending=True).groupby('SQL_ID', as_index=False).agg(lambda x: x.tolist()) #.sort_values(['PIECE'],ascending = True)
queries = querySort.SQL_TEXT
# print queries

myList = []
for i in queries:
    data1 = ''.join(i).replace(","," ").replace("("," ")
    data2 = data1+'\n'     # python will convert \n to os.linesep
    myList.append(data2)

# """breaking each query into 'FROM' to 'SELECT' and 'FROM' to end of the query"""
#
qryList = []
for k in myList:
    word = k.split(" ")
    words = filter(None, word)
    # print (words)
#
    if ("JOIN" or "WHERE") in words:
       for i in range(0, len(words)):
           if words[i].startswith('FROM'):
               subList = []
               while (True):
                   if (i == len(words)):
                       break
                   subList.append(words[i])
                   if words[i].endswith('SELECT'):
                       qryList.append(subList)
                       break
                   elif words[i].endswith(words[-1]) and words[-2] not in ("FROM", "from"):
                       qryList.append(subList)
                       break
                   else:
                       i += 1
# print(qryList)
# # #
"""getting the table names which must be the next to each 'FROM' and 'JOIN', alias names which are next to table names and finally
the elements having alias names + "." as column names
group the column names with same alias name"""
# #
mainList = []
for i in qryList:

   tableNames = []
   aliasNames = []
   colNames = []
   if ("JOIN" or "WHERE") in i:
       next_word = i[i.index("FROM") + 1]
       tableNames.append(next_word.lower())
   #print(tableNames)
       if i[i.index(next_word) + 1] not in ["WHERE"]:
           alias1 = i[i.index(next_word) + 1]
           aliasNames.append(alias1)
   #print aliasNames

       a = "JOIN"   # element to be found
       for index in range(len(i)):  # traversing through length of the list
           if i[index] == a:
               index = i[index+1]  #(this will give the next index word)
               tableNames.append(index.lower())
   # print(tableNames)
               if i[-1] == index:
                   aliasNames = aliasNames
               else:
                   alias2 = i[i.index(index) + 1]
                   aliasNames.append(alias2)
   # print(aliasNames)
   #
       for ch in i:
           for a in aliasNames:
               # print a
               if ch.startswith(a + "."):
                   # print ch
                   colNames.append(ch.lower())
   # print(colNames)
   #
       a = "WHERE"  # element to be found
       for index in range(len(i)):  # traversing through length of the list
           if i[index] == a:
               index = i[index + 1]  # (this will give the next index word)
               colNames.append(index)
               colNames = [re.sub('[^a-zA-Z0-9_.]', '', k) for k in colNames]  # remove all the unnecessary extra characters
   # print(colNames)
   # #

   x_sorted = sorted(colNames)
   x_sorted = [list(g) for k, g in groupby(x_sorted, key=lambda x: x[0] if (len(a) ==1) else x[1])] # sort and grop column names
   mainList.append([tableNames,x_sorted])

df = pd.DataFrame(columns=['tableName','colNames'],data = mainList) #create dataframe
dfNew = df[(df['colNames'].str.len() != 0)]

# """expand lists of column names with same alias names"""
#
def split_dataframe_rows(df, column_selectors):
   # we need to keep track of the ordering of the columns
   def _split_list_to_rows(row, row_accumulator, column_selector):
       split_rows = {}
       max_split = 0
       for column_selector in column_selectors:
           split_row = row[column_selector]
           split_rows[column_selector] = split_row
           if len(split_row) > max_split:
               max_split = len(split_row)

       for i in range(max_split):
           new_row = row.to_dict()
           for column_selector in column_selectors:
               try:
                   new_row[column_selector] = split_rows[column_selector].pop(0)
               except IndexError:
                   new_row[column_selector] = ''
           row_accumulator.append(new_row)

   new_rows = []
   df.apply(_split_list_to_rows, axis=1, args=(new_rows, column_selectors))
   new_df = pd.DataFrame(new_rows, columns=df.columns)
   return new_df


df2 = split_dataframe_rows(dfNew, column_selectors=dfNew)
dataFrame = df2[(df2['tableName'].str.len() != 0)]

"""expand each list in colnames, split with "." to seperate the alias names and get the column names only"""

s = dataFrame.apply(lambda x: pd.Series(x['colNames']), axis=1).stack().reset_index(level=1, drop=True)
s.name = 'colNames'
df2 = dataFrame.drop('colNames', axis=1).join(s)
df2['colNames'] = pd.Series(df2['colNames'], dtype=object)

df2['colNames'] = df2['colNames'].str.split('.').str[1] ## for splitting w.r.t '.' and taking the second element
df2['score'] =1       #initially, set that counter to 1.
group_data = df2.groupby(['tableName','colNames'])['score'].sum().sort_values(ascending=False) #group the data and count the occurance
# print(group_data)
# #
#"""saving in a csv"""
#
group_data.to_csv('D:/sparkProjects/data/output2.csv',header= True)

