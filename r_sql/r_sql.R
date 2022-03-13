
# load library 
if (!("rlang" %in% installed.packages()))
{
  install.packages("rlang")
}
if (!("DBI" %in% installed.packages()))
{
  install.packages("DBI")
}
if (!("dplyr" %in% installed.packages()))
{
  install.packages("dplyr")
}

library ("DBI")
library("dplyr")

test = TRUE

#dont remove the huge db when testing ... 
if (!test)
{
  #remove old db
  if(file.exists("N:\\Uni_Bigdata\\Harvard_Dataverse.db"))
  {
    file.remove("N:\\Uni_Bigdata\\Harvard_Dataverse.db")
  }
}

#make the connection
conn = dbConnect(RSQLite::SQLite(), "N:\\Uni_Bigdata\\Harvard_Dataverse.db")

#read csv's
airports = 
  read.csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\airports.csv", header = TRUE)
carriers = 
  read.csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\carriers.csv", header = TRUE)
planes = 
  read.csv("N:\\Google drive sync\\Uni\\Courses\\ST2195-Programming-for-data-science\\Data\\plane-data.csv", header = TRUE)

ontime_2001 = read.csv("N:\\Uni_Bigdata\\2001.csv") 
ontime_2002 = read.csv("N:\\Uni_Bigdata\\2002.csv")

dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)
dbWriteTable(conn, "ontime", ontime_2001)
dbWriteTable(conn, "ontime", ontime_2002, append = TRUE)

dbListTables(conn)

#1st q

dbGetQuery(conn,"
select carriers.Description, count(*) from ontime
inner join carriers
on carriers.code = ontime.UniqueCarrier
where ontime.cancelled = 1
group by carriers.Description
order by 2 desc")

#2nd q
dbGetQuery(conn,"
select carriers.Description, cast(sum(case when ontime.Cancelled = 1 then 1 else 0 end) as float) / count(*) from ontime
inner join carriers
on carriers.code = ontime.UniqueCarrier
group by carriers.Description
order by 2 desc")

#3rd q
dbGetQuery(conn,"
select planes.model, avg(ontime.DepDelay) from ontime
inner join planes
on ontime.tailnum = planes.tailnum
where ontime.DepDelay != 'NA'
and ontime.Cancelled = 0
and ontime.Diverted =0
group by planes.type
order by 2 desc")

#4th q
dbGetQuery(conn,"
select airports.city, count(*)
from airports 
inner join ontime 
on ontime.dest = airports.iata
where ontime.Cancelled = 0
group by airports.city
order by 2 desc")

#now same thing with dplyr notation

#create ref to the tables

airports_db = tbl(conn, "airports")

carriers_db = tbl(conn, "carriers")

planes_db = tbl(conn, "planes")

ontime_db = tbl(conn, "ontime")

#1st q

q1 = inner_join(carriers_db,ontime_db, by = c("Code" = "UniqueCarrier"), suffix = c(".carriers",".ontime")) %>%
                  filter(Cancelled==1) %>%
                  select(Description) %>%
                  group_by(Description) %>%
                  summarize(num = count()) %>%
                  arrange(desc(count()))


#2nd #3rd #4th similiar ,copy it --- todo todo todo

#disconnect the db

dbDisconnect(conn)