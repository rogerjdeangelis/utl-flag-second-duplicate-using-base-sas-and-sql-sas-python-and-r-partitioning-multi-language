%let pgm=utl-flag-second-duplicate-using-base-sas-and-sql-sas-python-and-r-partitioning-multi-language;

%stop_submission;

Flag second duplicate using base sas and sql sas python and r multi language

github
https://tinyurl.com/3tsjd4v9
https://github.com/rogerjdeangelis/utl-flag-second-duplicate-using-base-sas-and-sql-sas-python-and-r-partitioning-multi-language

Stackoverflow sas
https://tinyurl.com/4wutnbum
https://stackoverflow.com/questions/79144822/flag-duplicate-observations-in-sas


/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                    |                                      |                                            */
/*                                    |                                      |                                            */
/*              INPUT                 |       PROCESS                        |              OUTPUT                        */
/*              =====                 |       =======                        |              =======                       */
/*                                    |   SAS R AND PYHTON                   |                                            */
/*                                    |  DOES NOT REQUIRE PROC SORT          |                                    | ADD   */
/*                                    |                                      |                                    | THIS  */
/*    DUP GROUPS            IGNORE    |  USE SQL PATITIONING TO              |     DUP GROUPS            IGNORE   | FLAG  */
/* =====================    ========  |  SEQUENCE THE DUPS                   |  =====================    ======== | ====  */
/*       EVENT_                       |                                      |        EVENT_                      |       */
/* ID     DT_TM    EVENT    EVENT_ID  |  select                              |  ID     DT_TM    EVENT    EVENT_ID |  FLAG */
/*                                    |     ID                               |                                    |       */
/*  1      25      SaO2       621     |    ,event_ID                         |   1      25      SaO2       621    |   0   */
/*  1      25      SaO2       622     |    ,event_dt_tm                      |   1      25      SaO2       622    |   1   */
/*                                    |    ,event                            |                                    |       */
/*  2      24      RR         741     |    ,(partition=2) as flag_dup        |   2      24      RR         741    |   0   */
/*                                    |  from                                |                                    |       */
/*  2       3      HR         742     |     %sqlpartition(                   |   2       3      HR         742    |   0   */
/*                                    |  sd1.have,                           |                                    |       */
/*  3       4      NC         511     |     by=%str(id, event_dt_tm, event)) |   3       4      NC         511    |   0   */
/*  3       4      NC         512     |                                      |   3       4      NC         512    |   1   */
/*                                    |  BASE SAS                            |                                    |       */
/*  3      12      SaO2       513     |  ========                            |   3      12      SaO2       513    |   0   */
/*                                    |                                      |                                            */
/*                                    |  proc sort data=sd1.have out=havSrt; |                                            */
/*                                    |      by id event_dt_tm event;        |                                            */
/*                                    |  run;quit;                           |                                            */
/*                                    |                                      |                                            */
/*                                    |  data want;                          |                                            */
/*                                    |      set havSrt;                     |                                            */
/*                                    |      by id event_dt_tm event;        |                                            */
/*                                    |      flag_duplicate_event            |                                            */
/*                                    |         = (first.event = 0);         |                                            */
/*                                    |  run;quit;                           |                                            */
/*                                    |                                      |                                            */
/**************************************************************************************************************************/

    SOLUTIONS

       1 sas sql
       2 R sql
       3 Python sql
       4 base sas

SOAPBOX ON
SAS should add partioning to proc sql, has many uses.
SOAPBOX OFF


RELATED REPOS
-----------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-adding-sequence-numbers-and-partitions-in-SAS-sql-without-using-monotonic
https://github.com/rogerjdeangelis/utl-create-equally-spaced-values-using-partitioning-in-sql-wps-r-python
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
input ID event_dt_tm event$ event_ID$ ;
cards4;
1 25 SaO2 621
1 25 SaO2 622
2 24 RR   741
2 03 HR   742
3 04 NC   511
3 04 NC   512
3 12 SaO2 513
;;;;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*         EVENT_                                                                                                         */
/*   ID     DT_TM    EVENT    EVENT_ID                                                                                    */
/*                                                                                                                        */
/*    1      25      SaO2       621                                                                                       */
/*    1      25      SaO2       622                                                                                       */
/*    2      24      RR         741                                                                                       */
/*    2       3      HR         742                                                                                       */
/*    3       4      NC         511                                                                                       */
/*    3       4      NC         512                                                                                       */
/*    3      12      SaO2       513                                                                                       */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                             _
/ |  ___  __ _ ___   ___  __ _| |
| | / __|/ _` / __| / __|/ _` | |
| | \__ \ (_| \__ \ \__ \ (_| | |
|_| |___/\__,_|___/ |___/\__, |_|
                            |_|
*/

proc sql;
  create
    table want as
  select
     ID
    ,event_ID
    ,event_dt_tm
    ,event
    ,(partition=2) as flag_dup
  from
     %sqlpartition(
  sd1.have,
     by=%str(id, event_dt_tm, event))
;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                   EVENT_                                                                                               */
/* ID    EVENT_ID     DT_TM    EVENT    FLAG_DUP                                                                          */
/*                                                                                                                        */
/*  1      621         25      SaO2         0                                                                             */
/*  1      622         25      SaO2         1                                                                             */
/*  2      742          3      HR           0                                                                             */
/*  2      741         24      RR           0                                                                             */
/*  3      512          4      NC           0                                                                             */
/*  3      511          4      NC           1                                                                             */
/*  3      513         12      SaO2         0                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
want <- sqldf('
 select
   *
  ,(partition=2.) as flag_dup
 from
   (select *, row_number() OVER (PARTITION BY id, event_dt_tm, event) as partition from have )
 ')
want;
fn_tosas9x(
      inp    = want
     ,outlib ="d:/sd1/"
     ,outdsn ="rwant"
     )
;;;;
%utl_rendx;

proc print data=sd1.rwant;
run;quit;

/**************************************************************************************************************************/
/*                                                   |                                                                    */
/*  R                                                |                                                                    */
/*                                                   |  SAS                                                               */
/*  > want;                                          |                                                                    */
/*                                                   |                   EVENT_                                           */
/*ID EVENT_DT_TM EVENT EVENT_ID partition flag_dup   | ROWNAMES ID   DT_TM    EVENT    EVENT_ID    PARTITION    FLAG_DUP  */
/*                                                   |                                                                    */
/* 1          25  SaO2      621         1        0   |     1     1    25      SaO2       621           1            0     */
/* 1          25  SaO2      622         2        1   |     2     1    25      SaO2       622           2            1     */
/* 2           3    HR      742         1        0   |     3     2     3      HR         742           1            0     */
/* 2          24    RR      741         1        0   |     4     2    24      RR         741           1            0     */
/* 3           4    NC      511         1        0   |     5     3     4      NC         511           1            0     */
/* 3           4    NC      512         2        1   |     6     3     4      NC         512           2            1     */
/* 3          12  SaO2      513         1        0   |     7     3    12      SaO2       513           1            0     */
/*                                                   |                                                                    */
/**************************************************************************************************************************/

/*____               _   _                             _
|___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/


proc datasets lib=sd1 nolist nodetails;
 delete pywant;
run;quit;

%utl_pybeginx;
parmcards4;
exec(open('c:/oto/fn_python.py').read());
have,meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat');
have
want=pdsql('''
 select
   *
  ,1.0*(partition=2) as flag_dup
 from
   (select *, row_number() OVER (PARTITION BY id, event_dt_tm, event) as partition from have )
 ''')
print(want);
fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3);
;;;;
%utl_pyendx;

proc print data=sd1.pywant;
run;quit;

/**************************************************************************************************************************/
/*                                                          |                                                             */
/* PYTHON                                                   |  SAS                                                        */
/*                                                          |                                                             */
/*                                                          |        EVENT_                                               */
/*     ID  EVENT_DT_TM EVENT EVENT_ID  partition  flag_dup  |  ID     DT_TM    EVENT    EVENT_ID    PARTITION    FLAG_DUP */
/*                                                          |                                                             */
/* 0  1.0         25.0  SaO2      621          1       0.0  |   1      25      SaO2       621           1            0    */
/* 1  1.0         25.0  SaO2      622          2       1.0  |   1      25      SaO2       622           2            1    */
/* 2  2.0          3.0    HR      742          1       0.0  |   2       3      HR         742           1            0    */
/* 3  2.0         24.0    RR      741          1       0.0  |   2      24      RR         741           1            0    */
/* 4  3.0          4.0    NC      511          1       0.0  |   3       4      NC         511           1            0    */
/* 5  3.0          4.0    NC      512          2       1.0  |   3       4      NC         512           2            1    */
/* 6  3.0         12.0  SaO2      513          1       0.0  |   3      12      SaO2       513           1            0    */
/*                                                          |                                                             */
/**************************************************************************************************************************/

/*  _     _
| || |   | |__   __ _ ___  ___   ___  __ _ ___
| || |_  | `_ \ / _` / __|/ _ \ / __|/ _` / __|
|__   _| | |_) | (_| \__ \  __/ \__ \ (_| \__ \
   |_|   |_.__/ \__,_|___/\___| |___/\__,_|___/

*/

proc sort data=sd1.have out=havSrt;;
    by id event_dt_tm event;
run;

data want;
    set havSrt;
    by id event_dt_tm event;
    flag_duplicate_event = (first.event = 0);
run;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*                                                                                                                        */
/*         EVENT_                         FLAG_DUPLICATE_                                                                 */
/*   ID     DT_TM    EVENT    EVENT_ID         EVENT                                                                      */
/*                                                                                                                        */
/*    1      25      SaO2       621              0                                                                        */
/*    1      25      SaO2       622              1                                                                        */
/*    2       3      HR         742              0                                                                        */
/*    2      24      RR         741              0                                                                        */
/*    3       4      NC         511              0                                                                        */
/*    3       4      NC         512              1                                                                        */
/*    3      12      SaO2       513              0                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
