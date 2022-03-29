# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 17:38:01 2022

@author: Patri
"""

import os 
import sqlite3
import pandas as pd



test = False

# This makes sure you can run this notebook multiple times without errors
if test == False:
    try:
        os.remove('N:\\Uni_Bigdata\\Harvard_Dataverse.db')
    except OSError:
        pass


#create the connection / db
conn = sqlite3.connect("N:\\Uni_Bigdata\\Harvard_Dataverse.db")

#read csv's
airports = pd.read_csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\airports.csv")

carriers = pd.read_csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\carriers.csv")

planes = pd.read_csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\plane-data.csv")

ontime_2001 = pd.read_csv("N:\\Uni_Bigdata\\2001.csv", encoding='latin-1') 
ontime_2002 = pd.read_csv("N:\\Uni_Bigdata\\2002.csv", encoding='latin-1')

airports.to_sql('airports', con = conn, index = False) 
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False)


ontime_2001.to_sql('ontime', con = conn,index = False)
ontime_2002.to_sql('ontime', con = conn,index = False, if_exists='append')

for year in range(2001, 2006):
    ontime = pd.read_csv(str(year)+".csv",  encoding = "ISO-8859-1" )
    ontime.to_sql('ontime', con = conn, if_exists = 'append', index = False)

conn.commit()


curs = conn.cursor()

#1st q

q1 = curs.execute("""
select carriers.Description, count(*) from ontime
inner join carriers
on carriers.code = ontime.UniqueCarrier
where ontime.cancelled = 1
group by carriers.Description
order by 2 desc""").fetchall()

pd.DataFrame(q1)

#2nd q
q2 = curs.execute("""
select carriers.Description, cast(sum(case when ontime.Cancelled = 1 then 1 else 0 end) as float) / count(*) from ontime
inner join carriers
on carriers.code = ontime.UniqueCarrier
group by carriers.Description
order by 2 desc""").fetchall()

pd.DataFrame(q2)

#3rd q
q3 = curs.execute("""
select planes.model, avg(ontime.DepDelay) from ontime
inner join planes
on ontime.tailnum = planes.tailnum
where ontime.DepDelay != 'NA'
and ontime.Cancelled = 0
and ontime.Diverted =0
group by planes.type
order by 2 desc""").fetchall()

pd.DataFrame(q3)

#4th q
q4 = curs.execute("""
select airports.city, count(*)
from airports 
inner join ontime 
on ontime.dest = airports.iata
where ontime.Cancelled = 0
group by airports.city
order by 2 desc""").fetchall()

pd.DataFrame(q4)

#disconnect the db

conn.close()