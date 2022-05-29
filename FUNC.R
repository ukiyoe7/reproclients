## ATNDIMENTO POR CLIENTE

library(DBI)
library(dplyr)

## conexão com banco replica 
##test

con2 <- dbConnect(odbc::odbc(), "reproreplica")

func <- dbGetQuery(con2,"
WITH FUNCIO AS (SELECT FUNCODIGO,FUNNOME FROM FUNCIO)

SELECT CLICODIGO,FUNCODIGO2,FUNNOME FROM CLIEN C
INNER JOIN FUNCIO F ON C.FUNCODIGO2=F.FUNCODIGO
WHERE CLICLIENTE='S'
") 

View(func)
