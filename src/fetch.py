#!/usr/bin/env python3
#encoding: UTF-8
"""Script to image a section of the BLS's LABSTAT database."""

# modules
from pymysql import connect,DatabaseError,Warning
from pymysql.cursors import DictCursor
from sys import stderr,stdout,version_info
from os import getenv,chmod,unlink
from requests import get
from tempfile import NamedTemporaryFile
from warnings import filterwarnings
filterwarnings("ignore", message="numpy.dtype size changed")
filterwarnings("ignore", message="numpy.ufunc size changed")

def main():
    # arguments
    from argparse import ArgumentParser
    args=ArgumentParser();
    args.add_argument("-D","--database",type=str,default="database=Analysis",help='Database connection.') 
    args.add_argument("-T","--truncate",action='store_true',help="Drop tables.")
    args.add_argument("-U","--update",action='store_true',help="Set to update database.")
    args.add_argument("-H","--hidden",action='store_true',help="Prevent arguments and secrets being echoed to the terminal.")    
    args.add_argument("section",type=str,choices=['ap','cu','su'],help="Section of LABSTAT to extract.")
    args.add_argument("-u","--url",type=str,default='https://download.bls.gov/pub/time.series/%s',help="URL for data folder.")
    args.add_argument("-n","--newline",type=str,default=r'\r\n',help="Line break code for data files.")
    args.add_argument("-c","--column",type=str,default=r'\t',help="Column break code for data files.")
    args.add_argument("-I","--ignore",type=int,default=1,help="Header lines to ignore in files.")
    args.add_argument("-s","--series",type=str,default='%s.series',help="Name of data file for series metadata.")
    args.add_argument("-d","--data",type=str,default='%s.data.0.Current',help="Name of data file for time series.")
    args.add_argument("-k","--keep",action='store_false',help="Set to keep temporary files (usually for debugging).")
    args.add_argument("-i","--items",type=str,default="%s.item",help="Name of data file for item metadata.")
    args.add_argument("-w","--warnings",action='store_true',help="Set to catch database warnings.")
    args=args.parse_args();
    
    if "%s" in args.url:
        args.url=args.url % args.section
        
    if "%s" in args.series:
        args.series=args.series % args.section

    if "%s" in args.data:
        args.data=args.data % args.section
        
    if "%s" in args.items:
        args.items=args.items % args.section

    # initialize
    print(__doc__,"\nParameters:",vars(args) if not args.hidden else 'hidden')

    if not args.warnings:
        filterwarnings('ignore',category=Warning)
    
    # connect to database
    database=dict(map(lambda x:x.split('='),args.database.split(';')))

    if 'pwd' not in database or database['pwd']=='' or database['pwd']==None:
        database['pwd']=getenv('MYSQLPASSWORD')

    if database['pwd']==None or database['pwd']=='':
        from getpass import getpass
        database['pwd']=getpass('Database password:')

    if 'server' not in database or database['server']=='' or database['server']==None:
        database['server']='localhost'

    print("Connecting to database %s." % (database['server'] if not args.hidden else 'hidden'))

    connection=connect(
        db=database['database'] if 'database' in database else 'mysql',
        host=database['server'] if 'server' in database else 'localhost',
        port=int(database['port']) if 'port' in database else 3306,
        user=database['uid'] if 'uid' in database else getenv('USER'),
        password=database['pwd'],
        cursorclass=DictCursor,
        autocommit=True
    )

    # main code
    try:
        tables=['BLSItems','BLSSeries','BLSTimeSeries','BLSTimeSeriesHistory']
        
        with connection.cursor() as cursor:
            # drop tables
            if args.truncate:
                raise Exception("If you really want to drop the tables, comment out this line.")
            
                for table in reversed(tables):
                    print("WARNING: Dropping table %s if it exists." % table)
                    sql="DROP TABLE IF EXISTS %s" % table
                    cursor.execute(sql)
            
            # create BLSItems table
            print("Creating BLSItems table if needed.")
            sql="""/* CREATE BLSItems TABLE */
CREATE TABLE IF NOT EXISTS BLSItems
(
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    section char(2) null,
    parent_id bigint,
    children int default 0,
    item_code varchar(16) not null,
    item_name varchar(256),
    display_level int,
    selectable bit,
    sort_sequence int,
    key item_code_key (item_code),
    unique key unique_key (section,item_code),
    constraint parent_id_fkey foreign key (parent_id) references BLSItems (id)
)"""
            if args.update:
                cursor.execute(sql)  
                
            else:
                print(sql)

            # create BLSSeries table
            print("Creating BLSSeries table if needed.")
            sql="""/* CREATE BLSSeries TABLE */
CREATE TABLE IF NOT EXISTS BLSSeries 
(
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    section char(2) null,
    series_id varchar(17) not null,unique key unique_key (series_id),
    area_code varchar(4),key area_code_key (area_code),
    item_code varchar(716),key item_code_key (item_code),
    seasonal char(1),
    periodicity_code char(1),
    base_code char(1),
    base_period varchar(20),
    series_title text,fulltext key fulltext_key (series_title),
    footnote_codes varchar(16),
    begin_year int,
    begin_period varchar(3),
    end_year int,
    end_period varchar(3),
    begin_date date,
    end_date date,
    constraint BLSSeries_section_item_code_fkey foreign key (section,item_code) references BLSItems (section,item_code) on delete cascade
)"""
            if args.update:
                cursor.execute(sql)  
                
            else:
                print(sql)
            
            # create BLSTimeSeries and BLSTimeSeriesHistory tables if needed
            print("Creating BLSTimeSeries table if needed.")
            sql="""/* CREATE BLSTimeSeries TABLE */
CREATE TABLE IF NOT EXISTS BLSTimeSeries
(
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    series_id varchar(17) not null,key series_id_key (series_id),
    date date,key date_key (date),
    year int,
    period varchar(3),
    value decimal(15,3),
    footnote_codes varchar(16),
    unique key unique_key (series_id,date),
    constraint BLSTimeSeries_series_id_fkey foreign key (series_id) references BLSSeries (series_id) on delete cascade
)"""
            if args.update:
                cursor.execute(sql)  
                
            else:
                print(sql)
                        
            print("Creating BLSTimeSeriesHistory table if needed.")
            sql="""/* CREATE BLSTimeSeries TABLE */
CREATE TABLE IF NOT EXISTS BLSTimeSeriesHistory
(
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    seq int not null default 1,
    series_id varchar(17) not null,key series_id_key (series_id),
    date date,key date_key (date),
    year int,
    period varchar(3),
    value decimal(15,3),
    footnote_codes varchar(16),
    unique key unique_key (series_id,date,value), # use of value in unique key will make this version capture revisions
    constraint BLSTimeSeriesHistory_series_id_fkey foreign key (series_id) references BLSSeries (series_id) on delete cascade    
)"""
            if args.update:
                cursor.execute(sql)  
                
            else:
                print(sql)

            # download and process items data
            url="%s/%s" % (args.url,args.items)
            print("Fetching item metadata from %s" %url)
            response=get(url)
            
            if response.status_code/100!=2:
                raise Exception("Status code %d returned for URL %s" % (response.status_code,url))

            with NamedTemporaryFile(dir='/tmp',delete=args.keep,mode='w+') as tempfile:
                tempfile.write(response.text)
                tempfile.flush()
                chmod(tempfile.name,0o644) # change file permissions   
                
                if args.section=='ap':
                    sql="""/* LOAD BLSItems DATA */
LOAD DATA INFILE
    '%s'
REPLACE INTO TABLE
    BLSItems
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @item_code,
    @item_name
)
SET    
    item_code=TRIM(@item_code),
    item_name=TRIM(@item_name),
    section='%s'""" % (
                            tempfile.name,
                            args.column,
                            args.newline,
                            args.ignore,
                            args.section
                    )
    
                elif args.section in ['cu','su']:
                    sql="""/* LOAD BLSItems DATA */
LOAD DATA INFILE
    '%s'
REPLACE INTO TABLE
    BLSItems
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @item_code,
    @item_name,
    @display_level,
    @selectable,
    @sort_sequence
)
SET
    item_code=TRIM(@item_code),
    item_name=TRIM(REGEXP_REPLACE(@item_name,'\\\\s+',' ')),
    display_level=CASE WHEN LENGTH(TRIM(@display_level))>0 THEN @display_level+0 END,
    selectable=CASE @selectable WHEN 'T' THEN 1 WHEN 'F' THEN 0 END,
    sort_sequence=CASE WHEN LENGTH(TRIM(@sort_sequence))>0 THEN @sort_sequence+0 END,
    section='%s'""" % (
                            tempfile.name,
                            args.column,
                            args.newline,
                            args.ignore,
                            args.section
                    )                    
        
                else:
                    raise Exception("Don't know how to load data for BLS LABSTAT section '%s'." % args.section)

                print("Bulk load of items metadata into database for LABSTAT section '%s'." % args.section)
                
                if args.update:
                    cursor.execute(sql)  

                else:
                    print(sql)

            # download and process series metadata
            url="%s/%s" % (args.url,args.series)
            print("Fetching series metadata from %s" %url)
            response=get(url)
            
            if response.status_code/100!=2:
                raise Exception("Status code %d returned for URL %s" % (response.status_code,url))

            with NamedTemporaryFile(dir='/tmp',delete=args.keep,mode='w+') as tempfile:
                tempfile.write(response.text)
                tempfile.flush()
                chmod(tempfile.name,0o644) # change file permissions   
                
                if args.section=='ap':
                    sql="""/* LOAD BLSSeries DATA */
LOAD DATA INFILE
    '%s'
REPLACE INTO TABLE
    BLSSeries
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @series_id,
    @area_code,
    @item_code,
    @series_title,
    @footnote_codes,
    @begin_year,
    @begin_period,
    @end_year,
    @end_period
)
SET
    section='%s',
    series_id=TRIM(@series_id),
    area_code=TRIM(@area_code),
    item_code=TRIM(@item_code),
    series_title=TRIM(@series_title),
    footnote_codes=CASE WHEN LENGTH(TRIM(@footnote_codes))>0 THEN TRIM(@footnote_codes) END,
    begin_year=CASE WHEN LENGTH(TRIM(@begin_year))>0 THEN @begin_year+0 END,
    begin_period=CASE WHEN LENGTH(TRIM(@begin_period))>0 THEN @begin_period END,
    end_year=CASE WHEN LENGTH(TRIM(@end_year))>0 THEN @end_year+0 END,
    end_period=CASE WHEN LENGTH(TRIM(@end_period))>0 THEN @end_period END,
    begin_date=CASE
                    WHEN begin_year>0 AND begin_period LIKE 'M%%' AND begin_period<>'M13' THEN STR_TO_DATE(CONCAT(begin_year,begin_period,'01'),'%%YM%%m%%d') 
                    WHEN begin_year>0 AND begin_period='S01' THEN STR_TO_DATE(CONCAT(begin_year,'0101'),'%%Y%%m%%d') 
                    WHEN begin_year>0 AND begin_period='S02' THEN STR_TO_DATE(CONCAT(begin_year,'0701'),'%%Y%%m%%d')
                    WHEN begin_year>0 AND begin_period IN ('M13','S03') THEN STR_TO_DATE(CONCAT(begin_year,'0101'),'%%Y%%m%%d') 
                END,
    end_date=CASE 
                WHEN end_year>0 AND end_period LIKE 'M%%' AND end_period<>'M13' THEN LAST_DAY(STR_TO_DATE(CONCAT(end_year,end_period,'01'),'%%YM%%m%%d')) 
                WHEN end_year>0 AND end_period='S01' THEN STR_TO_DATE(CONCAT(begin_year,'0630'),'%%Y%%m%%d') 
                WHEN end_year>0 AND end_period='S02' THEN STR_TO_DATE(CONCAT(begin_year,'1231'),'%%Y%%m%%d') 
                WHEN begin_year>0 AND begin_period IN ('M13','S03') THEN STR_TO_DATE(CONCAT(begin_year,'1231'),'%%Y%%m%%d') 
              END,
    seasonal=CASE WHEN series_title LIKE '%%not seasonally adjusted%%' THEN 'U' ELSE 'S' END""" % (
                            tempfile.name,
                            args.column,
                            args.newline,
                            args.ignore,
                            args.section
                    )
                        
                elif args.section in ['cu','su']:
                    sql="""/* LOAD BLSSeries DATA */
LOAD DATA INFILE
    '%s'
REPLACE INTO TABLE
    BLSSeries
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @series_id,
    @area_code,
    @item_code,
    @seasonal,
    @periodicity_code,
    @base_code,
    @base_period,
    @series_title,
    @footnote_codes,
    @begin_year,
    @begin_period,
    @end_year,
    @end_period
)
SET
    section='%s',
    series_id=TRIM(@series_id),
    area_code=TRIM(@area_code),
    item_code=TRIM(@item_code),
    seasonal=CASE WHEN LENGTH(TRIM(@seasonal))>0 THEN TRIM(@seasonal) END,
    periodicity_code=CASE WHEN LENGTH(TRIM(@periodicity_code))>0 THEN TRIM(@periodicity_code) END,
    base_code=CASE WHEN LENGTH(TRIM(@base_code))>0 THEN TRIM(@base_code) END,
    base_period=CASE WHEN LENGTH(TRIM(@base_period))>0 THEN TRIM(@base_period) END,
    series_title=TRIM(@series_title),
    footnote_codes=CASE WHEN LENGTH(TRIM(@footnote_codes))>0 THEN TRIM(@footnote_codes) END,
    begin_year=CASE WHEN LENGTH(TRIM(@begin_year))>0 THEN @begin_year+0 END,
    begin_period=CASE WHEN LENGTH(TRIM(@begin_period))>0 THEN @begin_period END,
    end_year=CASE WHEN LENGTH(TRIM(@end_year))>0 THEN @end_year+0 END,
    end_period=CASE WHEN LENGTH(TRIM(@end_period))>0 THEN @end_period END,
    begin_date=CASE
                    WHEN begin_year>0 AND begin_period LIKE 'M%%' AND begin_period<>'M13' THEN STR_TO_DATE(CONCAT(begin_year,begin_period,'01'),'%%YM%%m%%d') 
                    WHEN begin_year>0 AND begin_period='S01' THEN STR_TO_DATE(CONCAT(begin_year,'0101'),'%%Y%%m%%d') 
                    WHEN begin_year>0 AND begin_period='S02' THEN STR_TO_DATE(CONCAT(begin_year,'0701'),'%%Y%%m%%d')
                    WHEN begin_year>0 AND begin_period IN ('M13','S03') THEN STR_TO_DATE(CONCAT(begin_year,'0101'),'%%Y%%m%%d') 
                END,
    end_date=CASE 
                WHEN end_year>0 AND end_period LIKE 'M%%' AND end_period<>'M13' THEN LAST_DAY(STR_TO_DATE(CONCAT(end_year,end_period,'01'),'%%YM%%m%%d')) 
                WHEN end_year>0 AND end_period='S01' THEN STR_TO_DATE(CONCAT(begin_year,'0630'),'%%Y%%m%%d') 
                WHEN end_year>0 AND end_period='S02' THEN STR_TO_DATE(CONCAT(begin_year,'1231'),'%%Y%%m%%d') 
                WHEN begin_year>0 AND begin_period IN ('M13','S03') THEN STR_TO_DATE(CONCAT(begin_year,'1231'),'%%Y%%m%%d') 
              END""" % (                            
                            tempfile.name,
                            args.column,
                            args.newline,
                            args.ignore,
                            args.section
                    )

                print("Bulk load of series metadata into database for LABSTAT section '%s'." % args.section)
                
                if args.update:
                    cursor.execute(sql)  

                else:
                    print(sql)
        
            # download and process time series data
            url="%s/%s" % (args.url,args.data)
            print("Fetching time series data from %s" % url)
            response=get(url)
            
            if response.status_code/100!=2:
                raise Exception("Status code %d returned for URL %s" % (response.status_code,url))

            with NamedTemporaryFile(dir='/tmp',delete=args.keep,mode='w+') as tempfile:
                tempfile.write(response.text)
                tempfile.flush()
                chmod(tempfile.name,0o644) # change file permissions  
                
                sql="""/* LOAD BLSTimeSeries DATA */
LOAD DATA INFILE
    '%s'
REPLACE INTO TABLE
    BLSTimeSeries
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @series_id,
    @year,
    @period,
    @value,
    @footnote_codes
)
SET
    series_id=TRIM(@series_id),
    year=CASE WHEN LENGTH(TRIM(@year))>0 THEN @year+0 END,
    period=CASE WHEN LENGTH(TRIM(@period))>0 THEN @period END,
    value=CASE WHEN LENGTH(TRIM(@value))>0 THEN @value+0e0 END,
    footnote_codes=CASE WHEN LENGTH(TRIM(@footnote_codes))>0 THEN TRIM(@footnote_codes) END,
    date=CASE 
             WHEN year>0 AND period LIKE 'M%%' AND period<>'M13' THEN LAST_DAY(STR_TO_DATE(CONCAT(year,period,'01'),'%%YM%%m%%d')) 
             WHEN year>0 AND period='S01' THEN STR_TO_DATE(CONCAT(year,'0630'),'%%Y%%m%%d')
             WHEN year>0 AND period IN ('M13','S02','S03') THEN STR_TO_DATE(CONCAT(year,'1231'),'%%Y%%m%%d')
         END""" % (
                    tempfile.name,
                    args.column,
                    args.newline,
                    args.ignore
                )

                print("Bulk load of time series data into database.")

                if args.update:
                    cursor.execute(sql)  

                else:
                    print(sql)
                
                sql="""/* LOAD BLSTimeSeriesHistory DATA */
LOAD DATA INFILE
    '%s'
IGNORE INTO TABLE
    BLSTimeSeriesHistory
FIELDS TERMINATED BY '%s'
LINES TERMINATED BY '%s'
IGNORE %d LINES
(
    @series_id,
    @year,
    @period,
    @value,
    @footnote_codes
)
SET
    series_id=TRIM(@series_id),
    year=CASE WHEN LENGTH(TRIM(@year))>0 THEN @year+0 END,
    period=CASE WHEN LENGTH(TRIM(@period))>0 THEN @period END,
    value=CASE WHEN LENGTH(TRIM(@value))>0 THEN @value END,
    footnote_codes=CASE WHEN LENGTH(TRIM(@footnote_codes))>0 THEN TRIM(@footnote_codes) END,
    date=CASE 
             WHEN year>0 AND period LIKE 'M%%' AND period<>'M13' THEN LAST_DAY(STR_TO_DATE(CONCAT(year,period,'01'),'%%YM%%m%%d')) 
             WHEN year>0 AND period='S01' THEN STR_TO_DATE(CONCAT(year,'0630'),'%%Y%%m%%d')
             WHEN year>0 AND period IN ('M13','S02','S03') THEN STR_TO_DATE(CONCAT(year,'1231'),'%%Y%%m%%d')
         END,
    seq=seq+1""" % (
                    tempfile.name,
                    args.column,
                    args.newline,
                    args.ignore
                )

                print("Bulk load of time series data into history table in database.")
                cursor.execute(sql)

            # trim any blank records from import processes
            for table in reversed(tables):
                print("Trimming any blank records for %s." % table)
                sql=r"DELETE FROM %s WHERE %s NOT RLIKE '\\w'" % (
                    table,
                    'item_code' if table=='BLSItems' else 'series_id'
                )
                
                if args.update:
                    cursor.execute(sql)  

                else:
                    print(sql)
                
    except DatabaseError:
        if 'sql' in locals():
            stdout.flush()
            stderr.write("Problem with SQL:\n%s\n" % sql)
            
        raise

    # done
    print("Done.")

# bootstrap
if __name__ == "__main__":
    assert(version_info.major>=3)
    main()
