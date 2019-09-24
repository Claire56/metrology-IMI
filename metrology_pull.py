import pandas as pd
#import pypyodbc as pyodbc
#import shutil
import os
import subprocess
import datetime
import sys, traceback
#import base64
import subprocess
################################################################################
python_file_path = os.path.dirname(os.path.realpath(__file__))
################################################################################

def lambda_(ctx):
    global logger
    global uuid
    global informatics_root_dir
    global metrology_root_dir
    result = {'isok':-1, 'error_message':'', 'uuid':0}
    date_now = datetime.datetime.now()
    date_string = date_now.strftime("%Y-%m-%d %H:%M:%S")
    df_config = pd.read_excel(python_file_path+r'\metrology_config.xlsx', sheet_name='config')
    informatics_root_dir = df_config['informatics_root_dir'].iloc[0]
    metrology_root_dir = df_config['metrology_root_dir'].iloc[0]
    try:
    #if True:
        uuid = ctx['uuid']
        result['uuid'] = uuid
        logger = ctx['logger']
        # Pull data from current month, and previous 2 months
        date_now = datetime.datetime.now()
        year1 = date_now.year
        month1 = date_now.month
        #
        year2 = year1
        month2 = month1 - 1
        if month2 == 0:
            month2 = 12
            year2 = year2 - 1
        #
        year3 = year2
        month3 = month2 - 1
        if month3 == 0:
            month3 = 12
            year3 = year3 - 1
        #
        s = '-'
        if month1 <= 9:
            s = '-0'
        year_month1 = str(year1) + s + str(month1)
        s = '-'
        if month2 <= 9:
            s = '-0'
        year_month2 = str(year2) + s + str(month2)
        s = '-'
        if month3 <= 9:
            s = '-0'
        year_month3 = str(year3) + s + str(month3)
        # Get data for Metrology tools for processed data
        # Read from Excel file, sheet = "pull"
        df_equip = pd.read_excel(python_file_path+r'\metrology_config.xlsx', sheet_name='pull')
        df_equip.columns = [x.lower() for x in df_equip.columns]
        df_equip = df_equip[df_equip['active'] == 1]
        df_equip['equipmentid'] = df_equip['equipmentid'].astype(int, errors='ignore')
        df_equip.set_index('equipmentid',drop=False,inplace=True)
        # set up network connection
        if os.popen('net use').read().find(metrology_root_dir) < 0:
            net_use = 'NET USE "' + metrology_root_dir + r'" $3rv1c3 /user:intermolecular\s_cf'
            subprocess.call(net_use, shell=True)   
            #
        for source_dir, sub_dirs, files in os.walk(metrology_root_dir):
            # Look for directories under the metrology root, of the format: 
            # <cdp_dir>\<metrology_data_dir>\<year-month>
            # Example: CDP123\XRD\2018-05
            cdp_dir = ''; data_dir = ''; date_dir = ''
            sub_dir_list = source_dir.replace(metrology_root_dir+'\\','').split('\\')
            if len(sub_dir_list) == 3:
                cdp_dir = sub_dir_list[0]
                data_dir = sub_dir_list[1]
                date_dir = sub_dir_list[2]
            if date_dir != year_month1 and date_dir != year_month2 and date_dir != year_month3:
                continue
            # If found then go through the lines of the excel file matching the 
            # metrology_data_dir and copy to Informatics with the format:
            # informatics_root_dir\informatics_data_dir\cdp_dir
            # Example: EquipmentData\XRD.Summary\CDP123
            for index, row in df_equip.iterrows():
                equipmentid = row['equipmentid']
                equipmentname = row['equipmentname']
                suffix = row['suffix']
                if suffix.find('*') != 0:
                    suffix = '*' + suffix
                metrology_data_dir = row['metrology_data_dir']
                informatics_data_dir = row['informatics_data_dir']
                if metrology_data_dir == data_dir:
                    destination_dir = informatics_root_dir + '\\' + informatics_data_dir + '\\' + cdp_dir
                    log_file_name = python_file_path+'\\log\\robocopy_'+cdp_dir+'_'+data_dir+'_'+date_dir+'.log'
                    #print('=============')
                    #print('source_dir = ' + source_dir)
                    #print('destination_dir = ' + destination_dir)
                    #print('suffix = ' + suffix)
                    #print('log_file_name = ' + log_file_name)
                    robocopy_result = robocopy(source_dir, destination_dir, suffix, log_file_name)
        result['isok'] = 1
        return result
    except Exception as ex:
        logger.error('****** Exception in metrology_pull.py ******')
        logger.error(date_string)
        logger.error("[" + __name__ + "]:" + str(ex), exc_info=True)
        logger.error('************************************')
        result['error_message'] = 'Exception occurred in metrology_pull.py'
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



def robocopy(source_dir, destination_dir, suffix, log_file_name):
    result = {'isok':-1, 'error_message':'', 'uuid':uuid}
    if True:
    #try:
        args = []
        args.append(r'C:\WINDOWS\system32\robocopy.exe')
        args.append(source_dir)
        args.append(destination_dir)
        args.append(suffix)
        args.append('/S')
        args.append('/R:0')
        args.append('/maxage:30')
        args.append('/LOG:' + log_file_name)
        # run executable
        # https://docs.python.org/3/library/subprocess.html
        robocopy_result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        result['robocopy_result'] = robocopy_result
        result['isok'] = 1
        return result
    '''
    except Exception as ex:
        result['error_message'] = 'Exception occurred in metrology_pull.py (robocopy)'
        logger.error("[" + __name__ + "]:" + str(ex), exc_info=True)
        result['Exception'] = str(ex)
        return result
    '''

'''
This is the command to set up the schedued task that runs this script:

$action = New-ScheduledTaskAction –Execute python C:\pydev\metrology\metrology_pull.py
$trigger = New-ScheduledTaskTrigger -Once -At 12:07am -RepetitionDuration (New-TimeSpan -Days 10000) -RepetitionInterval (New-TimeSpan -Minutes 5)
Register-ClusteredScheduledTask –Cluster STG-FILE.inf.intermolecular.local –TaskName metrology_pull –TaskType AnyNode –Action $action –Trigger $trigger
'''


if __name__ == '__main__':
    import sys, re
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=python_file_path+r'\log\metrology_pull.log',level=logging.DEBUG)

    ctx= {}
    ctx['logger'] = logger
    ctx['uuid'] = 0
    ctx['args'] = {}
    
    result = lambda_(ctx)
    #print(result)

