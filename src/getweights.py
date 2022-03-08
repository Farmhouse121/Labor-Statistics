#!/usr/bin/env python3
#encoding: UTF-8
"""Script to download BLS Relative Importance Weights for CPI."""
# modules
from pymysql import connect,DatabaseError,Warning,OperationalError
from pymysql.cursors import DictCursor
from sys import stderr,stdout,version_info
from os import getenv,chmod
from datetime import datetime,timedelta
from requests import get
from xlrd import open_workbook
from re import sub,search
from warnings import filterwarnings
filterwarnings("ignore", message="numpy.dtype size changed")
filterwarnings("ignore", message="numpy.ufunc size changed")

class Break(Exception):
    """Permits deep breaks."""
    def __str__(self):
        return "Break"

def main():
    # arguments
    from argparse import ArgumentParser
    args=ArgumentParser();
    args.add_argument("-D","--database",type=str,default="database=Analysis",help='Database connection.')
    args.add_argument("-H","--hidden",action='store_true',help="Prevent arguments and secrets being echoed to the terminal.")    
    args.add_argument("-U","--update",action='store_true',help='Set to update records that already exist, otherwise they are ignored.')
    args.add_argument("-u","--url",type=str,default="https://www.bls.gov/cpi/tables/supplemental-files/news-release-table2-%s.xlsx",help="URL to get CPI worksheets.")
    args.add_argument("-p","--period",type=str,default=None,help="Period to download data for.")
    args.add_argument("-o","--offset",type=int,default=2,help="Periods to offset by.")
    args.add_argument("-f","--folder",type=str,default="/Users/%s/Dropbox/Data/BLS" % getenv("USER"),help="Folder for data storage.")
    args.add_argument("-s","--sheet",type=int,default=0,help="Worksheet index in Excel workbook.")
    args.add_argument("-r","--read",type=str,default='cu',help="Section of LABSTAT to read for items.")
    args.add_argument("-w","--write",type=str,default='W%',help="Section of LABSTAT to write items to (should not be an official section.")
    args.add_argument("-F","--firstrow",type=int,default=6,help="First data row in table (0 offset).")
    args.add_argument("-C","--category",type=int,default=1,help="Data column for category (0 offset).")
    args.add_argument("-W","--weight",type=int,default=2,help="Data column for weight (0 offset).")
    args.add_argument("-L","--label",type=int,default=3,help="Data row for reference period label (0 offset).")
    args.add_argument("-I","--indent",type=int,default=0,help="Data row for indent level (0 offset).")
    args.add_argument("-x","--warnings",action='store_true',help="Set to catch database warnings.")    
    args.add_argument("-n","--nodownload",action='store_true',help="Set to prevent downloads.")
    args=args.parse_args();
    
    if args.period==None:
        dt,om=datetime.strptime(datetime.now().strftime("%Y%m28"),"%Y%m%d"),args.offset
        
        while om>0:
            dt-=timedelta(days=29)
            dt=datetime.strptime(dt.strftime("%Y%m28"),"%Y%m%d")
            om-=1
            
        args.period=dt.strftime("%Y%m")
        
    if "%s" in args.url:
        args.url=args.url % args.period

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
        with connection.cursor() as cursor:
            # read BLSItems for cu section
            items,sql={},"SELECT * FROM BLSItems WHERE section='%s'" % args.read
            cursor.execute(sql)
            
            for row in cursor.fetchall():
                items[row['item_name']]=row
                
            print("Found %d items in section '%s'." % (len(items),args.read))
            
            # download the data
            filename="%s/%s" % (args.folder,args.url.split("/")[-1])

            if not args.nodownload:
                print("Fetching data file from:",args.url)
                response=get(args.url)

                if response.status_code/100!=2:
                    raise ValueError("Status code %d returned for URL %s" % (response.status_code,args.url))

                print("Writing data to %s" % filename)

                with open(filename,"wb") as datafile:
                    for chunk in response.iter_content():
                        datafile.write(chunk)

                    chmod(datafile.name,0o644) # change file permissions  
                
            if args.update:
               # process the data
                print("Reading data from local cached copy.")
            
                with open_workbook(filename,"r") as workbook:
                    worksheet=workbook.sheets()[args.sheet]
                    nr,nc=worksheet.nrows,worksheet.ncols                 
                    print("Sheet is %d rows by %d columns." % (nr,nc))
                    s=sub(r"[\r\n]+"," ",worksheet.cell(args.label,args.weight).value)
                    m=search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[.\s]+(\d+)",s)
                    
                    if m!=None:
                        dt=datetime.strptime("%s %s 01" % m.group(1,2),"%b %Y %d")
                    
                    else:
                        raise ValueError("Cannot identify reference period in label string '%s' in Cell(%d,%d)." % (s,args.label,args.weight))
                
                    print("Reference period:",dt.strftime("%YM%m"))
                    
                    path,sseq=[],[]
                    
                    for row in range(args.firstrow,nr):
                        if worksheet.cell(row,args.indent).ctype==2:
                            i=worksheet.cell(row,args.indent).value
                            c=sub(r"\s*\(\d+\)\s*","",worksheet.cell(row,args.category).value)
                            w=worksheet.cell(row,args.weight).value
                            level=i+1

                            if level>len(path):
                                path.append(c)

                            else:
                                while level<=len(path):
                                    path.pop()

                                path.append(c)
                            
                            if c in items:
                                sseq.append(c)
                                items[c]['indent_level']=i
                                items[c]['value']=w if worksheet.cell(row,args.weight).ctype==2 else None                               
                                items[c]['sort_sequence']=sseq.index(c)+1

                                if i>0 and path[-2] in items:
                                    sql="SELECT * FROM BLSItems WHERE section='%s' AND item_code='%s'" % (
                                        args.write,
                                        items[path[-2]]['item_code']
                                    )
                                                                        
                                    cursor.execute(sql)
                                    
                                    for row in cursor.fetchall():
                                        items[c]['parent_id']=row['id']
                                
                                else:
                                    items[c]['parent_id']=None
                                
                    print("Writing data.")
                    
                    for item,data in sorted(items.items(),key=lambda x:x[1]['sort_sequence']): 
                        if 'indent_level' not in data:
                            continue
                            
                        # items 
                        fields={
                            'section':"'%s'" % args.write,
                            'item_code':"'%s'" % data['item_code'],
                            'item_name':"'%s'" % data['item_name'].replace("'","''"),
                            'display_level':str(data['indent_level']),
                            'selectable':str(ord(data['selectable'])),
                            'sort_sequence':str(data['sort_sequence']),
                            'children':'0'
                        }
                                                
                        if data['parent_id']!=None:
                            fields['parent_id']=str(data['parent_id'])
                                                
                        sql="/* Writing data to '%s' section of BLSItems table */ INSERT INTO BLSItems (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (
                            args.write, # this is encoded as a comment for debugging purposes
                            ",".join(fields.keys()),
                            ",".join(fields.values()),
                            ",".join(map(lambda x:"%s=%s" % x,fields.items()))
                        )

                        if args.update:
                            cursor.execute(sql) 

                        else:
                            print(sql)
                            
                        if data['parent_id']!=None and data['value']!=None:    
                            sql="UPDATE BLSItems SET children=children+1 WHERE id=%s" % fields['parent_id']
                        
                            if args.update:
                                cursor.execute(sql) 

                            else:
                                print(sql)

                        # series
                        fields={
                            'section':"'%s'" % args.write,
                            'area_code':"'0000'",
                            'seasonal':"'U'",
                            'periodicity_code':"'R'",
                            'item_code':"'%s'" % data['item_code'],
                        }
                        
                        fields['series_id']="'%2.2s%1.1s%1.1s%4.4s%s'" % (
                            fields['section'].replace("'",""),
                            fields['seasonal'].replace("'",""),
                            fields['periodicity_code'].replace("'",""),
                            fields['area_code'].replace("'",""),
                            fields['item_code'].replace("'","")
                        )
                        
                        fields['series_title']="'Relative importance of %s, not seasonally adjusted'" % item.replace("'","''")                            
                        sql="/* Writing data to '%s' section of BLSSeries table */ INSERT INTO BLSSeries (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (
                            args.write, # this is encoded as a comment for debugging purposes
                            ",".join(fields.keys()),
                            ",".join(fields.values()),
                            ",".join(map(lambda x:"%s=%s" % x,fields.items()))
                        )

                        if args.update:
                            cursor.execute(sql) 

                        else:
                            print(sql)
                            
                        # write to BLSTimeSeries table 
                        fields={
                            'series_id':fields['series_id'],
                            'date':dt.strftime("LAST_DAY('%Y-%m-%d')"),
                            'year':dt.strftime("'%Y'"),
                            'period':dt.strftime("'M%m'"),
                            'value':str(data['value']) if data['value']!=None else 'NULL'
                        }

                        sql="/* Writing data to BLSTimeSeries table */ INSERT INTO BLSTimeSeries (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (
                            ",".join(fields.keys()),
                            ",".join(fields.values()),
                            ",".join(map(lambda x:"%s=%s" % x,fields.items()))
                        )

                        if args.update:
                            cursor.execute(sql) 

                        else:
                            print(sql)
                            
                        sql="/* Writing data to BLSTimeSeriesHistory table */ INSERT IGNORE INTO BLSTimeSeriesHistory (%s) VALUES (%s)" % (
                            ",".join(fields.keys()),
                            ",".join(fields.values())
                        )

                        if args.update:
                            cursor.execute(sql) 

                        else:
                            print(sql)

                        print("%3d %1d %8.8s %-8.8s %s" % (
                            data['sort_sequence'],
                            data['indent_level'],
                            ("%8.3f" % data['value']) if data['value']!=None else "",
                            data['item_code'],
                            (' '*int(data['indent_level']))+item
                        ))
 
    except (DatabaseError,OperationalError) as e:
        if 'sql' in locals():
            stdout.flush()
            stderr.write("Problem with SQL:\n%s\n%s\n" % (sql,str(e)))
        raise
    
    except KeyboardInterrupt:
        stdout.flush()
        stderr.write('Interrupted.\n')

    # done
    print("Done.")

# bootstrap
if __name__ == "__main__":
    assert(version_info.major>=3)
    main()
