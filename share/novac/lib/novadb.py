import re, sys, ConfigParser

from collections import OrderedDict
from sqlalchemy import create_engine

def execute(statement):
    # db = _db_connection()
    # results = db.execute(statement)
    # db.dispose()
    results = statement
    return results

def _db_credentials():
    parser = ConfigParser.SafeConfigParser()
    if '/root/.my.cnf' in parser.read('/root/.my.cnf'):
        { "user": parser.get("client", "user"), "pass": parser.get("client", "password") }
    else:
        { "user": raw_input("Enter MySQL user: "), "pass": raw_input("Enter MySQL password: ") }

def _db_connection(db="nova"):
    creds = _db_credentials()
    creds["db"] = db
    connection_string = "mysql://%(user)s:%(pass)s@localhost/%(db)s" % creds 
    try:
        create_engine(connection_string)
    except:
        sys.exit("Couldn't connect to database! "
                 "[connection string: %s]" % 
                 connection_string)

def instance_name_constraint(instance_name, alias=None):
    col_prefix =  alias + "." if alias else ""

    int_re = re.compile(r'^[1-9]+$')
    hex_re = re.compile(r'^instance-[a-f0-9]{8}$')
    uuid_re = re.compile(r'^[0-9a-f]{8}-'
                                '[0-9a-f]{4}-'
                                '[0-9a-f]{4}-'
                                '[0-9a-f]{4}-[0-9a-f]{12}$')

    if int_re.match(instance_name):
        instance_id = int(instance_name[-8:], 16)
        return "%sid = %s" % (col_prefix, instance_id)
    elif hex_re.match(instance_name):
        instance_id = int(instance_name)
        return "%sid = %s" % (col_prefix, instance_id)
    elif uuid_re.match(instance_name):
        return "%suuid = '%s'" % (col_prefix, instance_name)
    else:
        return "%sdisplay_name LIKE '%%%%%s%%%%'" % (col_prefix, instance_name)


