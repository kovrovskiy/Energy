#ver.140420221619
#PDU Raritan PXE-1966
import configparser, os, sys, logging
from select import select
from snmp_cmds import snmpwalk
from datetime import datetime
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer


current_datetime = datetime.now().date()
current_date = datetime.now()

logging.basicConfig(
    level=logging.DEBUG, filename="./log.txt",
    format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s',
)
logger = logging.getLogger("./log.txt")

#Reading a configuration file
if os.path.exists("conf.ini"):
    config = configparser.ConfigParser()
    config.read("conf.ini")
    community = config["snmp"]["community"]
    oid = config["snmp"]["oid"]
    file_ipAddr = config["ip"]["ipaddr"]
else:
    logger.error("Config file not found")
    sys.exit()

#snmp request function
def result_oid(comm, ipaddr, oid_value):
    return snmpwalk(community= comm, ipaddress= ipaddr, oid= oid_value)  

#unction to retrieve data from the database
def select_energy_value(date):
    return select([energy_table.c.date,\
            energy_table.c.ip,\
                energy_table.c.data]).\
                            where(energy_table.c.ip == line.strip()).\
                                where(energy_table.c.date == date)
           
#function of writing data from the database
def insert_energy_value(ip_addr, oid_value):

    return energy_table.insert().\
            values(date=current_datetime, ip=ip_addr, data=int(oid_value))


#function to delete data from the database
def delete_energy_value(date):
    return energy_table.delete().\
        where(energy_table.c.ip == line.strip()).\
            where(energy_table.c.date == date)

#database write function
def wr_data_db():
    conn.execute(insert_energy_value(line.strip(), str(oid_request).rsplit("'")[3]))

#Conn to sql db
metadata_snmp = MetaData()
energy_table = Table('ENERGY_VALUE', metadata_snmp, Column('date', Integer),\
     Column('ip', Integer),\
          Column('data', Integer))
enrg = create_engine("sqlite:///snmpdata.db")
metadata_snmp.create_all(enrg)

#Reading a ip list
if os.path.exists(file_ipAddr):
    conn = enrg.connect()
    ipAddr = open(file_ipAddr, 'r')
    lines = ipAddr.readlines()
    #We receive data via snmp and write it to the database
    for line in lines:
        try:
            logger.info(f'wrdata: Starting receiving data from {line}')
            oid_request = result_oid(community, line.strip(), oid)
            if conn.execute(select_energy_value(current_datetime)).fetchall() == None:
               wr_data_db()
               logger.info(f"wrdata: Successfully received data from {line}")
            else:
                conn.execute(delete_energy_value(current_datetime))
                wr_data_db()
                logger.info(f"wrdata: Successfully received data from {line}")
        except Exception as e:
            oid_request = None
            logger.error(e)
    #Total amount and total energy consumption    
else:
    logger.error("File ip.list not found")
    sys.exit()

