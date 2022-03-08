# Interacting with the MySQL Database

The `--database` switch on input uses an "ODBC like" connection string, it may contain the fields:

    database=database_name;server=server_name;uid=user_name;pwd=password;port=tcp_port
    
in any order, or optionally omitting some. If the code fails to find `UID` it will try the environment variable `USER`;
if it fails to find `PWD` it will try the environment variable `MYSQLPASSWORD`, and if that fails it will try to read
the password from the command line via the Python `getpass` system (which doesn't echo text).

The code uses `NamedTemporaryFile` to create downloaded text files for bulk insert into the database via `LOAD DATA INFILE`.
For `LOAD DATA INFILE` to work the server must have `secure_file_priv = "/tmp"` set in the `my.cnf` file, which can be edited
either manually or via the MySQL Workbench GUI. This permits the database to read data from that folder. If you specify `""`
you are enabling *any* folder.

Since you might want to specify a password on the command line, and the code echoes it's parameters to the screen in normal 
operations, you can use the `--hidden` switch to stop it doing that.
