import pandas as pd
import pypyodbc as pyodbc
import shutil
import os
import subprocess
import datetime
import sys, traceback
import base64
################################################################################
python_file_path = os.path.dirname(os.path.realpath(__file__))
################################################################################

def lambda_(ctx):
    global logger
    global date_now
    global date_string
    global informatics_db_server
    global metrology_root_dir
    result = {'isok':-1, 'error_message':'', 'uuid':0}
    date_now = datetime.datetime.now()
    date_string = date_now.strftime("%Y-%m-%d %H:%M:%S")
    df_config = pd.read_excel(python_file_path+r'\metrology_config.xlsx', sheet_name='config')
    informatics_db_server = df_config['informatics_db_server'].iloc[0]
    metrology_root_dir = df_config['metrology_root_dir'].iloc[0]
    #
    try:
    #if True:
        ### Get data for Metrology tools; read from Excel file ###
        #df_equip = pd.read_sql_query(sql_get_equip(),db_imdb)
        df_equip = pd.read_excel(python_file_path+r'\metrology_config.xlsx', sheet_name='push')
        df_equip.columns = [x.lower() for x in df_equip.columns]
        df_equip = df_equip[df_equip['active'] == 1]
        df_equip['equipmentid'] = df_equip['equipmentid'].astype(int, errors='ignore')
        df_equip.set_index('equipmentid',drop=False,inplace=True)
        equip_id_list = df_equip['equipmentid'].tolist()
        equip_id_string = ','.join(str(e) for e in equip_id_list)
        #
        df_email = pd.DataFrame(columns=('fileid','source','destination', \
                            'created','lastmodified','copy_result','comment'))
        #print(equip_id_list)
        #print(equip_id_string)
        #print(df_equip)
        #return result
        ### Database connection ###
        db_config = DBConfig()
        db_imdb = pyodbc.connect(db_config['IMDB_CONN_STRING'])
        cur_imdb = db_imdb.cursor()
        # get set of files to be copied
        #### Set start date ###
        #start_date = '2018-05-28'
        window = 7 # days
        start_date = (date_now - datetime.timedelta(days=window)).strftime("%Y-%m-%d")
        df_files = pd.read_sql_query(sql_get_files(start_date,equip_id_string),db_imdb)
        #print(df_files)
        #return result
        # loop through file list
        ### set up network connection ###
        if os.popen('net use').read().find(metrology_root_dir) < 0:
            net_use = 'NET USE "' + metrology_root_dir + r'" $3rv1c3 /user:intermolecular\s_cf'
            subprocess.call(net_use, shell=True)
        ### Loop through file list ###
        for index, row in df_files.iterrows():
            # DataFileLog row data
            fileid = row['fileid']
            equipmentid = row['equipmentid']
            filename = row['filename']
            datadir = row['datadir']
            filefullpath = row['filefullpath']
            created = row['created']
            lastmodified = row['lastmodified']
            lastlogged = row['lastlogged']
            previous_copy_result = row['result']
            equipmentname = df_equip.at[equipmentid, 'equipmentname']
            metrology_data_dir = df_equip.at[equipmentid, 'metrology_data_dir']
            suffix = df_equip.at[equipmentid, 'suffix']
            ### Check that source file exists ###
            ### If not, make an entry in the db and skip to the next record ###
            #print(filefullpath)
            if not os.path.exists(filefullpath):
                if previous_copy_result == None:
                    copy_result = -1
                    # Don't try to copy again
                    comment = 'Source file not found'
                    destination = ''
                    sql = sql_metrology_insert(fileid, destination, created, 
                                            lastmodified, copy_result, comment)
                    db_result = cur_imdb.execute(sql)
                    cur_imdb.commit()
                continue
            ### metrology CDP directory
            try:
                cdp_number = int(filename[0:3])
            except:
                cdp_number = 0
            if cdp_number <= 0:
                '''
                ### Can't determine CDP
                ### Don't copy but make an entry in db and skip to next record
                if previous_copy_result == None:
                    copy_result = -1
                    ### Don't try to copy again
                    comment = 'Bad file name format'
                    destination = ''
                    sql = sql_metrology_insert(fileid, destination, created, 
                                            lastmodified, copy_result, comment)
                    db_result = cur_imdb.execute(sql)
                    cur_imdb.commit()
                continue
                '''
                metrology_cdp_dir = 'NoCDP'
            elif cdp_number == 1:
                metrology_cdp_dir = 'Intermolecular'
            elif cdp_number <= 9:
                metrology_cdp_dir = 'CDP00' + str(cdp_number)
            elif cdp_number <= 99:
                metrology_cdp_dir = 'CDP0' + str(cdp_number)
            else:
                metrology_cdp_dir = 'CDP' + str(cdp_number)
            met_dir = metrology_root_dir + '\\' + metrology_cdp_dir
            if os.path.isdir(met_dir) == False:
                os.mkdir(met_dir)
            # metrology data directory
            # Special case for XRD/XRR
            if metrology_data_dir == 'XRD,XRR':
                if filename.find('XRR') >= 0 or datadir.find('XRR') >= 0:
                    metrology_data_dir = 'XRR'
                else:
                    metrology_data_dir = 'XRD'
            # Special case for SEM/EDS
            if metrology_data_dir == 'SEM,EDS':
                if datadir.find('EDS_Data') >= 0:
                    metrology_data_dir = 'EDS'
                else:
                    metrology_data_dir = 'SEM'
            met_dir = met_dir + '\\' + metrology_data_dir
            if os.path.isdir(met_dir) == False:
                os.mkdir(met_dir)
            # sub directory with year-month
            met_month_dir = str(created)[0:7]
            met_dir = met_dir + '\\' + met_month_dir
            if os.path.isdir(met_dir) == False:
                os.mkdir(met_dir)
            # metrology file name
            n = filename.rindex('.'); s1 = filename[:n]; s2 = filename[n:]
            met_filename = s1 + '_' + suffix + s2
            # destination full path and file name
            destination = met_dir + '\\' + met_filename
            ### copy file ###
            copy_return = shutil.copy2(filefullpath, destination)
            # For Ellipsometer, every .txt file is associated with a .SE file
            # which is not in the DataFileLog table; copy this as well
            if metrology_data_dir == 'ELLIPSOMETRY' and filefullpath.endswith('.txt'):
                filefullpath2 = filefullpath.replace('.txt','.SE')
                destination2 = destination.replace('.txt','.SE')
                if os.path.exists(filefullpath2):
                    copy_return2 = shutil.copy2(filefullpath2, destination2)
            #################
            # check if copy succeeded
            if os.path.exists(destination):
                copy_result = 1
                # copy successful
                comment = ''
            else:
                copy_result = 0
                # Attempted to copy but failed
                # Will attempt to copy again next time
                comment = 'ERROR: File not detected at destination'
                # Make an entry that will be sent in email at the end
                df_email = df_email.append( { 'fileid':fileid,
                    'source':filefullpath, 'destination':destination,
                    'created':created, 'lastmodified':lastmodified,
                    'copy_result':copy_result, 'comment':comment }, ignore_index=True)
            # write to DataFileMetrology table
            if previous_copy_result == None:
                # New file, not previously copied
                sql = sql_metrology_insert(fileid, destination, created, 
                                            lastmodified, copy_result, comment)
            else:
                if copy_result == 1 and previous_copy_result == 1:
                    comment = 'New version of previously copied file'
                sql = sql_metrology_update(fileid, destination, created, 
                                            lastmodified, copy_result, comment)
            db_result = cur_imdb.execute(sql)
            cur_imdb.commit()
        # close database connection
        db_imdb.close()
        # send email if errors have been generated
        if df_email.shape[0] > 0:
            with pd.option_context('display.max_colwidth', -1):
                output_html = df_email.to_html()
            send_metrology_email(output_html)
        # close network connection
        # on second thoughts, let's just leave it open as we're running so frequently
        #if prev_met_root_dir != None:
        #    net_use = 'NET USE "' + prev_met_root_dir + r'" /DELETE'
        #    subprocess.call(net_use, shell=True)
        # finished
        result['isok'] = 1
        return result
    except Exception as ex:
        logger.error('****** Exception in metrology_push.py ******')
        logger.error(date_string)
        logger.error("[" + __name__ + "]:" + str(ex), exc_info=True)
        logger.error('************************************')
        result['error_message'] = 'Exception occurred in metrology_push.py'
        result['Exception'] = str(ex)
        # https://docs.python.org/3/library/traceback.html
        exc_type, exc_value, exc_traceback = sys.exc_info()
        result['exc_type'] = repr(exc_type)
        result['exc_value'] = repr(exc_value)
        tb = traceback.format_tb(exc_traceback, limit=None)
        n = len(tb); s1 = tb[n-1]; s2 = s1.strip().split('\n'); i = 0
        for s in s2:
            i += 1; result['exc_traceback'+str(i)] = s.strip()
        return result



def DBConfig():
    IMDB_SERVER = informatics_db_server
    IMDB_DB = 'IMDB'
    IMDB_USER = 'sa'
    if informatics_db_server == 'dev-db-01':
        IMDB_PWD = base64.b64decode(b'aW50ZXJtb2xlY3VsYXIx').decode('utf-8')
    else:
        IMDB_PWD = base64.b64decode(b'U3Rhclcwcno=').decode('utf-8')
    IMDB_CONN_STRING = 'Driver={SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;' %(IMDB_SERVER, IMDB_DB, IMDB_USER, IMDB_PWD)
    return { 'IMDB_CONN_STRING' : IMDB_CONN_STRING }
'''
def sql_get_equip():
    return ("SELECT e.EquipmentID, e.EquipmentName, et.EquipmentType "
    "FROM Equipment e "
    "JOIN EquipmentType et on e.EquipmentTypeID = et.EquipmentTypeID "
    "WHERE e.EquipmentID in (" + equip_id_string + ") "
    "ORDER BY e.EquipmentName" )
'''
def sql_get_files(date_string, equip_id_string):
    return ("SELECT dfl.FileID,dfl.Created,dfl.LastModified,dfl.LastLogged,"
    "dfl.EquipmentID,dfl.DataDir,dfl.FileName,dfl.FileFullPath,dfm.Result "
    "FROM DataFileLog dfl "
    "LEFT JOIN DataFileMetrology dfm ON dfl.FileID = dfm.FileID "
    "WHERE dfl.EquipmentID in (" + equip_id_string + ") "
    "AND dfl.LastModified >= '" + date_string + "' "
    "AND ( dfm.FileID IS NULL " # new file not copied
    "OR dfm.Result = 0 " # previous copy attempt failed
    "OR DATEDIFF(second, dfm.LastModified, dfl.LastModified) > 1.0 )" ) 
                            # newer version of previously copied file

# As usual, dates are problematic. For a good overview of dates in Python see:
# https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64/13753918#13753918

def sql_metrology_insert(fileid, destination, created, lastmodified, result, comment):
    return ("INSERT INTO DataFileMetrology "
    "(FileID,Destination,Created,LastModified,DateCopied,Result,Comment) "
    "VALUES (" + str(fileid) + ",'"
    + destination + "','"
    + str(created)[0:23] + "','"
    + str(lastmodified)[0:23] + "',"
    + "SYSDATETIME(),"
    + str(result) + ",'"
    + comment + "')" )

def sql_metrology_update(fileid, destination, created, lastmodified, result, comment):
    return ("UPDATE DataFileMetrology "
    "SET Destination = '" + destination + "', "
    "Created = '" + str(created)[0:23] + "', "
    "LastModified = '" + str(lastmodified)[0:23] + "', "
    "DateCopied = SYSDATETIME(), "
    "Result = " + str(result) + ", "
    "Comment = '" + comment + "' "
    "WHERE FileID = " + str(fileid) )


def send_metrology_email(message):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    # https://en.wikibooks.org/wiki/Python_Programming/Email
    server = smtplib.SMTP('mail.intermolecular.com', 25)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login("intermolecular\s_cf", "$3rv1c3")
    #
    from_addr = 'Python Metrology <s_cf@intermolecular.com>'
    to_addrs = 'George.Li@intermolecular.com,malcolm.mcgregor@intermolecular.com'
    #to_addrs = 'malcolm.mcgregor@intermolecular.com,George.Li@intermolecular.com'
    #to_addrs = 'pipeline.pilot@intermolecular.com, malcolm.mcgregor@intermolecular.com'
    #cc_addrs = 'malcolm.mcgregor@intermolecular.com'
    #
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addrs
    #msg['Cc'] = cc_addrs
    msg['Subject'] = 'Message from Python Metrology Service'
    #
    body = '<i>This is an automated message from Informatics; '
    body = body + 'please do not repond to this message; '
    body = body + 'instead contact a member of the informatics team in the '
    body = body + 'recipents above with any question or issues.</i><br><br>'
    body = body + '<b>There were some error messages generated when attempting '
    body = body + 'to copy from the Informatics server to the Metrology server; '
    body = body + 'see below.</b><br><br>'
    body = body + message
    #
    msg.attach(MIMEText(body, 'html'))
    text = msg.as_string()
    #s = server.sendmail(from_addr, to_addrs, text)
    server.send_message(msg)



'''
Notes:
The code only reads from 1 database table in IMDB:
- the existing DataFileLog table.
It writes to only 1 table:
- the newly created DataFileMetrology table, that only this app uses.

Script for creating DataFileMetrology table:

CREATE TABLE DataFileMetrology (
FileID int NOT NULL PRIMARY KEY,
Destination varchar(500),
Created datetime,
LastModified datetime,
DateCopied datetime,
Result int,
Comment varchar (255)
);

Description of database table fields:
Each row is a file that corresponds to the DataFileLog table.
    FileID : from DataFileLog table
    Destination : full file name and path to metrology folder
    Created : from DataFileLog table
    LastModified : from DataFileLog table
                - used to determine if a new file version is available
    DateCopied : timestamp of entry
    Result : 
        1 = File successfully copied and detected at destination
        0 = Copy attempt failed; file not detected at destination; 
            will attempt to copy again next time
        -1 = Copy not attempted for known reason, eg. wrong file name format; 
            will NOT attempt to copy again
    Comment : additional information such as error messages

'''



if __name__ == '__main__':
    import sys, re
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=python_file_path+r'\log\metrology_push.log',level=logging.DEBUG)

    ctx= {}
    ctx['logger'] = logger
    ctx['uuid'] = 0
    ctx['args'] = {}
    
    result = lambda_(ctx)
    #print(result)
