#!/usr/bin/env python3
#encoding: UTF-8
"""Script to fetch and upload CPI taxonomies and weights."""

# modules
from pymysql import connect,DatabaseError
from pymysql.cursors import DictCursor
from sys import stderr,stdout,version_info
from os import getenv
from requests import get
from re import match,sub

def main():
    # arguments
    from argparse import ArgumentParser
    args=ArgumentParser();
    args.add_argument("-D","--database",type=str,default="database=Analysis",help='Database connection.')
    args.add_argument("-H","--hidden",action='store_true',help="Prevent arguments and secrets being echoed to the terminal.")
    args.add_argument("-U","--update",action='store_true',help='Set to update records that already exist, otherwise they are ignored.')
    args.add_argument("-T","--truncate",action='store_true',help="Drop and recreate data tables.")
    args.add_argument("year",type=int,nargs='+',help="Years to process.")
    args.add_argument("-B","--baseurl",type=str,default="https://www.bls.gov/cpi/tables/relative-importance/%d.txt",help='Base URL to fetch data from.')
    args.add_argument("-p","--pattern",type=str,default=r"^(\s+)([^.]+)\s?\.*\s+(\d*\.\d+)\s+(\d*\.\d+)",help="Pattern to identify data lines.")
    args.add_argument("-n","--newline",type=str,default="\n",help="Record terminator (newline).")
    args.add_argument("-d","--delimiter",type=str,default="/",help="String to delimit path in category hierarchy.")
    args=args.parse_args();

    # initialize
    print(__doc__,"\nParameters:",vars(args) if not args.hidden else 'hidden')

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
    sql,last,top,path,ids=None,"",False,[],{}

    try:
        with connection.cursor() as cursor:
            # schema
            if args.truncate:
                print("WARNING: Dropping and recreating data tables.")

                for table in ['CPICategories']:
                    sql='DROP TABLE IF EXISTS %s' % table
                    cursor.execute(sql)
                    print("Dropped %s." % table)

                sql="""/* CREATE CPICategories TABLE */
CREATE TABLE IF NOT EXISTS
    CPICategories
(
    id bigint not null primary key auto_increment,
    timestamp timestamp not null default current_timestamp on update current_timestamp,
    captured datetime not null default current_timestamp,
    parent_id bigint null,foreign key (parent_id) references CPICategories (id),
    children int not null default 0,
    Year int not null,key Year_key (Year),
    IndexName varchar(16) not null,key Index_key (IndexName),
    Code varchar(16) null,
    Level int null,
    Category varchar(256) not null,key Category_key (Category),
    Weight double not null,
    Path varchar(1024) null,
    md5 char(32) as (md5(Path)) stored,
    unique key unique_key (md5)
)"""
                cursor.execute(sql)
                print("Created CPICategories table.")

            for year in args.year:
                url=args.baseurl % year if "%d" in args.baseurl else args.baseurl
                print("Fetching data from",url)

                response=get(url)

                if response.status_code/100!=2:
                    raise Exception("HTTP Status Code %d for GET %s" % (response.status_code,url))

                for line in response.text.split(args.newline):
                    category=match(args.pattern,last+line)

                    if category!=None: # data lines
                        last,fields="",{
                            'Year':"%d" % year,
                            'Level':"%d" % len(category.group(1)),
                            'Category':"'%s'" % sub(r"\s+$","",sub(r"\s{2,}"," ",category.group(2))).replace("'","''")
                        }

                        level,data=int(fields['Level']),{
                            'CPI-U':category.group(3),
                            'CPI-W':category.group(4)
                        }

                        if level==1:
                            if top:
                                break

                            else:
                                top=True

                        if level>len(path):
                            path.append(sub(r"^'(.+)'$",r"\1",fields['Category']))

                        else:
                            while level<=len(path):
                                path.pop()

                            path.append(sub(r"^'(.+)'$",r"\1",fields['Category']))

                        for k,v in data.items():
                            fields['IndexName']="'%s'" % k
                            fields['Weight']=v
                            fields['Path']="'%s'" % args.delimiter.join([str(year),k]+path)

                            sql="SELECT * FROM CPICategories WHERE md5=MD5('%s')" % args.delimiter.join([str(year),k]+path[:-1])
                            cursor.execute(sql)

                            for row in cursor.fetchall():
                                fields['parent_id']=str(row['id'])

                            sql="INSERT INTO CPICategories (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (
                                ",".join(fields.keys()),
                                ",".join(fields.values()),
                                ",".join(map(lambda x:"%s=%s" % x,fields.items()))
                            )

                            if args.update:
                                cursor.execute(sql)

                            else:
                                print(sql)

                            # maintain the children list
                            if 'parent_id' in fields:
                                sql="UPDATE CPICategories SET children=children+1 WHERE id=%s" % fields['parent_id']

                                if args.update:
                                    cursor.execute(sql)

                                else:
                                    print(sql)

                    elif match(r"^\s+$",line)==None: # not a blank line
                        last=line

                    else: # it's a blank line
                        last=""

    except DatabaseError:
        stdout.flush()
        stderr.write("Problem with SQL:\n%s\n" % sql)
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
