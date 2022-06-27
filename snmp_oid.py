#ver.140420221619
#PDU Raritan PXE-1966
import configparser, os, sys, smtplib, logging
from select import select
from snmp_cmds import snmpwalk
from datetime import datetime
from sqlalchemy import create_engine, select, MetaData, Table, Column, Integer
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    day_receipt = int(config["sort"]["daysort"])
    file_ipAddr = config["ip"]["ipaddr"]
    priceList = float(config["EnergyPrice"]["price"])
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

#unction to retrieve data from the database
def select_energy_sum(date):
    return select([energy_table_SUM.c.date,\
            energy_table_SUM.c.ip,\
                energy_table_SUM.c.dataSum,\
                    energy_table_SUM.c.TotalSum]).\
                        where(energy_table_SUM.c.date == date)
            
#function of writing data from the database
def insert_energy_value(ip_addr, oid_value):

    return energy_table.insert().\
            values(date=current_datetime, ip=ip_addr, data=int(oid_value))

#function of writing data from the database
def insert_energy_sum(ip_addr, sum_value, prc_enrg_ttl):
    return energy_table_SUM.insert().\
            values(date=current_datetime, ip=ip_addr, dataSum=sum_value, priceAct=priceList, TotalSum=prc_enrg_ttl)

#function to delete data from the database
def delete_energy_value(date):
    return energy_table.delete().\
        where(energy_table.c.ip == line.strip()).\
            where(energy_table.c.date == date)

#function to delete data from the database
def delete_energy_value_Sum(date):
    return energy_table_SUM.delete().\
        where(energy_table_SUM.c.ip == line.strip()).\
            where(energy_table_SUM.c.date == date)

#send email function
def smtp_sender(energy_per_month, cost_per_month):
    addr_from = config["email"]["email_from_addr"]              
    addr_to   = config["email"]["emails_to_addr"]                    
    password  = config["email"]["smtp_user_pass"]                                 
    msg = MIMEMultipart()                              
    msg['From']    = addr_from                         
    msg['To']      = addr_to                            
    msg['Subject'] = 'Потребление электроэнергии '+str(config["name"]["name_corp"])

    body = "Потребление за месяц: "+str(energy_per_month)+" кВат\n"\
    "Сумма: "+str(cost_per_month)+" руб.\n"\
    "Сумма рассчитывается из стоимости "+str(priceList)+" руб/кВат \n\n\n"\
    "*Автоматическая система получения данных электропотребления"
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP(config["email"]["smtp_server"], config["email"]["smtp_port"])           
    #server.set_debuglevel(True)                         
    if config["email"]["smtp_ssl"] == "yes":
        server.starttls()
    else:
        logger.warning("An unencrypted connection is used")                             
    server.login(config["email"]["smtp_user"], password)               
    server.send_message(msg)                           
    server.quit()

#database write function
def wr_data_db():
    conn.execute(insert_energy_value(line.strip(), str(oid_request).rsplit("'")[3]))
    result_select_receipt = conn.execute(select_energy_value(date_receipt)).fetchall()
    result_select_current = conn.execute(select_energy_value(current_datetime)).fetchall()
    for row_receipt in result_select_receipt:
        for row_current in result_select_current:
            energy_receipt_total = row_current[2]-row_receipt[2]
            price_enrg_total = energy_receipt_total/1000*priceList
            if conn.execute(select_energy_sum(current_datetime)).fetchall() == None:
                conn.execute(insert_energy_sum(line.strip(),energy_receipt_total, price_enrg_total))
            else:
                conn.execute(delete_energy_value_Sum(current_datetime))
                conn.execute(insert_energy_sum(line.strip(),energy_receipt_total, price_enrg_total))

#We get the date of data selection
if 1 <= day_receipt <= 31:

    if current_date.month == 1:
        month_receipt = current_date.month+11
        year_receipt = current_date.year-1
        date_receipt = datetime(year_receipt, month_receipt-1, day_receipt).strftime("%Y-%m-%d")
    else :
        month_receipt = current_date.month
        year_receipt = current_date.year
        date_receipt = datetime(year_receipt, month_receipt-1, day_receipt).strftime("%Y-%m-%d")
else:
    try:
        day_receipt
    except Exception as curday:
        logger.error(curday)
    sys.exit()

#Conn to sql db
metadata_snmp = MetaData()
energy_table = Table('ENERGY_VALUE', metadata_snmp, Column('date', Integer),\
     Column('ip', Integer),\
          Column('data', Integer))
energy_table_SUM = Table('ENERGY_SUM', metadata_snmp, Column('date', Integer),\
     Column('ip', Integer),\
        Column('dataSum', Integer),\
            Column('priceAct', Integer),\
                Column('TotalSum', Integer))
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
            oid_request = result_oid(community, line.strip(), oid)
            if conn.execute(select_energy_value(current_datetime)).fetchall() == None:
               wr_data_db()
            else:
                conn.execute(delete_energy_value(current_datetime))
                wr_data_db()
        except Exception as e:
            oid_request = None
            logger.error(e)
    #Total amount and total energy consumption
    total_sum = conn.execute(select_energy_sum(current_datetime)).fetchall()
    sumData = 0
    sumTotalPrice = 0
    for i in total_sum:
        sumData = sumData + i[2]
        sumTotalPrice = sumTotalPrice +i[3]
    ipAddr.close()
    #Sending by mail
    try:
        smtp_email = smtp_sender(sumData/1000, str(sumTotalPrice).rsplit('.')[0])
    except Exception as esmtp:
        smtp_email = None
        logger.error(esmtp)
else:
    logger.error("File ip.list not found")
    sys.exit()

