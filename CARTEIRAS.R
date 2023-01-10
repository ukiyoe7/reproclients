library(googlesheets4)
library(DBI)
library(tidyverse)
con2 <- dbConnect(odbc::odbc(), "reproreplica")

## CLIENTES ======================================================================


cli <- dbGetQuery(con2,"SELECT 
                           DISTINCT C.CLICODIGO,
                            CLINOMEFANT,
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


clien %>% filter(!is.na(SETOR)) %>% tally()

anti_join(clien %>% filter(!is.na(SETOR)),carteiras_2,by="CLICODIGO")



## FATURAMENTO ========================================================================

result_cli <- dbGetQuery(con2,"
  
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

PED AS (SELECT ID_PEDIDO,PEDID.CLICODIGO,PEDDTBAIXA FROM PEDID 
INNER JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
   WHERE PEDDTBAIXA BETWEEN '01.01.2022' AND '31.12.2022' AND 
     PEDSITPED<>'C' AND 
      PEDLCFINANC IN ('S', 'L','N') 
        )

SELECT CLICODIGO,SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
 FROM PDPRD PD
  INNER JOIN PED P ON P.ID_PEDIDO=PD.ID_PEDIDO
  GROUP BY 1

")

## PRIMEIRA CONFIGURACAO ==========================================================

carteiras <- union_all(
read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR1") %>% mutate(SETOR="SETOR 1 - GDE FLORIANOPOLIS  - REDES") ,
read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR2")  %>% mutate(SETOR="SETOR 2 - CRICIUMA - SUL")) %>% 
            union_all(
              .,
              read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR3") %>% mutate(SETOR="SETOR 3 - CHAPECO - OESTE - RS") ) %>%  
             union_all(
               . ,
               read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR4") %>% mutate(SETOR="SETOR 4 - JOINVILLE - NORTE") )  %>% 
            union_all(
              .,
              read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR5") %>% mutate(SETOR="SETOR 5 - BLUMENAU - VALE") ) %>%  
            union_all(
              . ,
              read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR6") %>% mutate(SETOR="SETOR 6 - BALNEARIO CAMBORIU - LITORAL") )  %>% 
            union_all(
              .,
              read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR7") %>% mutate(SETOR="SETOR 7 - GDE FLORIANOPOLIS - INDEPENDENTES") ) %>%  
            union_all(
              . ,
              read_sheet("1J2TCW6dSkbOezmYvLK_DXhpz3K9ySsVd-Bmh1QNY9iI", sheet = "SETOR8") %>% mutate(SETOR="SETOR 8 - JOACABA - MEIO OESTE")) 
                        


clien %>% filter(!is.na(SETOR)) %>% View()



left_join(clien %>% filter(!is.na(SETOR)),carteiras %>% .[,c(1,7)],by="CLICODIGO") %>% View()


cidades <-left_join(clien %>% filter(!is.na(SETOR)),carteiras %>% .[,c(1,7)],by="CLICODIGO") %>% 
  group_by(SETOR.y) %>% distinct(CIDNOME)%>% arrange(CIDNOME) %>% as.data.frame()
  
range_write()
  
  range_write(cidades,ss = "1zO6XzE7mYwzdTM0xydCfLuVjeRWav5vjH1BVxrbYe3Q",sheet = "CIDADES",range = "A1")


cidades_carteiras <- read_sheet("1zO6XzE7mYwzdTM0xydCfLuVjeRWav5vjH1BVxrbYe3Q",sheet = "CIDADES")

View(cidades_carteiras)
  

## CRIAR CARTEIRAS ===============================================================


SETOR_1 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 1 - GDE FLORIANOPOLIS  - REDES") %>% left_join(.,result_cli,by="CLICODIGO") 

range_write(SETOR_1,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 1",range = "A1")


SETOR_2 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 2 - CRICIUMA - SUL") %>% left_join(.,result_cli,by="CLICODIGO") 

range_write(SETOR_2,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 2",range = "A1")


SETOR_3 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 3 - CHAPECO - OESTE - RS") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_3)

range_write(SETOR_3,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 3",range = "A1")


SETOR_4 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 4 - JOINVILLE - NORTE") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_4)

range_write(SETOR_4,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 4",range = "A1")


SETOR_5 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 5 - BLUMENAU - VALE") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_5)

range_write(SETOR_5,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 5",range = "A1")


SETOR_6 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 6 - BALNEARIO CAMBORIU - LITORAL") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_6)

range_write(SETOR_6,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 6",range = "A1")


SETOR_7 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 7 - GDE FLORIANOPOLIS - INDEPENDENTES") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_7)

range_write(SETOR_7,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 7",range = "A1")


SETOR_8 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 8 - JOACABA - MEIO OESTE") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_8)

range_write(SETOR_8,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 8",range = "A1")


SETOR_9 <- left_join(clien %>% filter(!is.na(SETOR)),cidades_carteiras,by="CIDNOME")  %>% 
  filter(SETOR.y=="SETOR 9 - ZONA NEUTRA - OUTRAS UFS - LABS") %>% left_join(.,result_cli,by="CLICODIGO") 

View(SETOR_9)

range_write(SETOR_9,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "SETOR 9",range = "A1")


carteiras_2 <- union_all(
  read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 1") ,
 
   read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 2")) %>% 
  union_all(
    .,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 3")) %>%  
  union_all(
    . ,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 4"))  %>% 
  union_all(
    .,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 5")) %>%  
  union_all(
    . ,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 6"))  %>% 
  union_all(
    .,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 7")) %>%  
  union_all(
    . ,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 8")) %>%  
  union_all(
    . ,
    read_sheet("1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4", sheet = "SETOR 9")) 


carteiras_2 %>% mutate(SETOR_FINAL=if_else(is.na(`AJUSTE SETOR`),SETOR.y,`AJUSTE SETOR`)) 


resumo_carteiras <-  carteiras_2 %>% 
                      mutate(SETOR_FINAL=if_else(is.na(`AJUSTE SETOR`),SETOR.y,`AJUSTE SETOR`))  %>% 
                       mutate(VRVENDA = ifelse(is.na(VRVENDA), 0, VRVENDA)) %>% 
                        as.data.frame() %>% 
                          group_by(SETOR_FINAL) %>% 
                            summarize(QTD= n_distinct(CLICODIGO), 
                              VRVENDA=sum(VRVENDA)) 

View(resumo_carteiras)


range_write(resumo_carteiras,ss = "1C88vCpyb0G4B6C2fv_Fym8UDdioXjuQECZlb5JsOcr4",sheet = "RESUMO",range = "A1")



## CRIAR CARTEIRAS REVISAO ===============================================================


SETOR_1_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 1 - GDE FLORIANOPOLIS  - REDES") %>% .[,c(1,2,3,4,6,8,10)] 

range_write(SETOR_1,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 1",range = "A1")


SETOR_2_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 2 - CRICIUMA - SUL") %>% .[,c(1,2,3,4,6,8,10)] 

range_write(SETOR_2,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 2",range = "A1")


SETOR_3_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 3 - CHAPECO - OESTE - RS") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_3)

range_write(SETOR_3,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 3",range = "A1")


SETOR_4_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 4 - JOINVILLE - NORTE") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_4)

range_write(SETOR_4,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 4",range = "A1")


SETOR_5_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 5 - BLUMENAU - VALE") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_5)

range_write(SETOR_5,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 5",range = "A1")


SETOR_6_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 6 - BALNEARIO CAMBORIU - LITORAL") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_6)

range_write(SETOR_6,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 6",range = "A1")


SETOR_7_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 7 - GDE FLORIANOPOLIS - INDEPENDENTES") %>% .[,c(1,2,3,4,6,8,10)]  

View(SETOR_7)

range_write(SETOR_7,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 7",range = "A1")


SETOR_8_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 8 - JOACABA - MEIO OESTE") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_8)

range_write(SETOR_8,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 8",range = "A1")


SETOR_9_v2 <- carteiras_2 %>% 
  filter(SETOR_FINAL=="SETOR 9 - ZONA NEUTRA - OUTRAS UFS - LABS") %>% .[,c(1,2,3,4,6,8,10)] 

View(SETOR_9)

range_write(SETOR_9,ss = "1m0VNU9HTRx_-bLRwLGljrIE7ZS5GmhM10LHXeyt9P4g",sheet = "SETOR 9",range = "A1")

















