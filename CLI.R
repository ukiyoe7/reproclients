library(DBI)
library(dplyr)
library(googlesheets4)

 con2 <- dbConnect(odbc::odbc(), "reproreplica")

 cli <- dbGetQuery(con2,"SELECT 
                           DISTINCT C.CLICODIGO,
                            CLINOMEFANT,
                             ENDCODIGO,
                              C.GCLCODIGO,
                               GCLNOME,
                                SETOR,
                                 CIDNOME
                                  FROM CLIEN C
                                   LEFT JOIN (SELECT CLICODIGO,
                                                     E.ZOCODIGO,
                                                      ZODESCRICAO SETOR,
                                                       ENDCODIGO,
                                                        CIDNOME 
                                                         FROM ENDCLI E
                                   LEFT JOIN CIDADE CD ON E.CIDCODIGO=CD.CIDCODIGO
                                    LEFT JOIN (SELECT ZOCODIGO,
                                                      ZODESCRICAO 
                                                        FROM ZONA WHERE ZOCODIGO IN (20,21,22,23,24,25,28))Z ON E.ZOCODIGO=Z.ZOCODIGO WHERE ENDFAT='S')A ON C.CLICODIGO=A.CLICODIGO
                                    LEFT JOIN GRUPOCLI GR ON C.GCLCODIGO=GR.GCLCODIGO
                                      WHERE CLICLIENTE='S'")
 
 View(cli)


 inativos <- dbGetQuery(con2,"
SELECT DISTINCT SITCLI.CLICODIGO,SITCODIGO FROM SITCLI
INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,MAX(SITDATA)ULTIMA FROM SITCLI
GROUP BY 1)A ON SITCLI.CLICODIGO=A.CLICODIGO AND A.ULTIMA=SITCLI.SITDATA 
INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,SITDATA,MAX(SITSEQ)USEQ FROM SITCLI
GROUP BY 1,2)MSEQ ON A.CLICODIGO=MSEQ.CLICODIGO AND MSEQ.SITDATA=A.ULTIMA 
AND MSEQ.USEQ=SITCLI.SITSEQ WHERE SITCODIGO=4
")
 
clien <- anti_join(cli,inativos,by="CLICODIGO") 

View(clien)

c1 <- clien %>% group_by(GCLCODIGO,GCLNOME) %>% summarize(C=n_distinct(CLICODIGO)) %>% as.data.frame() %>% 
        filter(C==1)

c2 <- left_join(c1,clien %>% select(GCLCODIGO,SETOR),by="GCLCODIGO")


range_write(c1,ss= "1TAkHnDOHeeTGr6G_WwIIVd3gGf2J1usHNu3V1USukaw",range = "A1",sheet="DADOS",reformat = FALSE)

range_write(c2,ss= "1TAkHnDOHeeTGr6G_WwIIVd3gGf2J1usHNu3V1USukaw",range = "A1",sheet="DADOS2",reformat = FALSE)

range_write(clien,ss= "1TAkHnDOHeeTGr6G_WwIIVd3gGf2J1usHNu3V1USukaw",range = "A1",sheet="DADOS3",reformat = FALSE)



