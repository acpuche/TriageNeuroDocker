#!/usr/bin/env python
# coding: utf-8
"""
Created on Thu May  6 13:16:21 2021

@author: neurofuncional
"""

#from build_and_send_hl7.py import builAndSedHL7

import pandas as pd
import numpy as np
import sys
import json
import hl7apy
from hl7apy import core
from hl7.client import MLLPClient
import hl7
import socket
import time
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
import run_RM_craneo_triageModel

from run_RM_craneo_triageModel import getDataFromDirectory_and_predict_production

#execute: python main.py ${INPUTDIR} ${BASEDIR}


class builAndSedHL7:

    '''
    The HL7-ORU message is generated and sent. The construction requires the header of the
    dicom image (in json format), the path of the Afga codes and the path of the radiologist
    codes (Excel files). Additionally, the model prediction value (0 or 1),
    the IP address and the destination port are required.

    Example: prueba = builAndSedHL7('test_info_dicom_xnat.json',0,'agfa_sap_code.xlsx','radiologist.xlsx',
    '172.25.121.208',8084)

    prueba.buildORM() # constructed message

    prueba.sendMessage() # Sending message
    '''

    def __init__(self,path_dicom_header,prediction,path_dat_code,
                 path_dat_radiologist,host,port):

        self.path_dicom_header = path_dicom_header
        self.prediction = prediction
        self.path_dat_code = path_dat_code
        self.path_dat_radiologist = path_dat_radiologist
        self.host = host
        self.port = port


    def readJson(self):
        '''
        Read a json file into a dictionary
        inputs:
            -path_dicom_header:path of json file with dicom header
        output:
            - output: dictionary objet

        Example:
                prueba = builAndSedHL7('test_info_dicom_xnat.json',0,'agfa_sap_code.xlsx','radiologist.xlsx','172.25.121.208',8084)
                prueba.buildORM() # constructed message
                prueba.sendMessage() # Sending message
        '''
        #Read dicom header
        with open(self.path_dicom_header) as json_file:
            header_dicom = json.load(json_file)

        return(header_dicom)


    def buildORM(self):
        '''
        Build a ORM hl7 message
        input:
            - path_dicom_header: Header of DICOM image (json)
            - prediction: Prediction of model (0: healty, 1: pathologic)
            - df_code: Master of Agfa codes
            - df_radiologist: Master of radiologist
        '''

        dat = self.readJson() # Dicom header
        agfa_sap = pd.read_excel(self.path_dat_code,dtype={'cod_equipo': np.str, 'prestacion': np.str,'cod_agfa':np.str},engine='openpyxl')
        df_radiologist = pd.read_excel(self.path_dat_radiologist,dtype={'ref': np.str},engine='openpyxl')

        if(self.prediction==0):
            pred = 'SANO'
        elif(self.prediction==1):
            pred = 'PATOLOGICO'
        else:
            raise TypeError('Prediction value must be 0 or 1.')

        #===========================================================================================
        #====================== Data extraction ====================================================
        #===========================================================================================

        x = dat['00081032']['Value'][0]['00080100']['Value'][0] # cups code
        cod_dat = agfa_sap[agfa_sap['cod_equipo']==x][['cod_agfa','descripcion']]
        if(cod_dat.shape[0]==1):
            cod_agfa = cod_dat.cod_agfa.values[0]
            description = cod_dat.descripcion.values[0]
        elif(cod_dat.shape[0]==0):
            raise Exception("Sorry, study code does'nt exist, please check master table.")
        elif(cod_dat.shape[0]>1):
            raise TypeError("There are duplicate codes in the master code table, please check.")

        doc_identidad = dat['00100020']['Value'][0]
        nombres = dat['00100010']['Value'][0]['Alphabetic'].split('^')
        birth = dat['00100030']['Value'][0]
        sex = dat['00100040']['Value'][0]
        order = '111111'
        nombres_r = dat['00080090']['Value'][0]['Alphabetic'].split('^')
        surnames_r = nombres_r[0]
        names_r = nombres_r[1].split(' ')
        fecha_estudio = dat['00080022']['Value'][0]+dat['00080032']['Value'][0].split('.')[0]
        accession_number = dat['00080050']['Value'][0]
        equipo = dat['00400241']['Value'][0]

        if(equipo=='RESONADOR1'):
            sede_id = 'HURESON'
        elif(equipo=='RESONADOR2'):
            sede_id = 'HRRESON'
        else:
            raise Exception('Sorry, the id modality is incorrect.')


        nam_r = nombres_r[0]+' '+nombres_r[1]
        df_radiologist['nam'] = df_radiologist['surname1']+' '+df_radiologist['surname2']+' '+ df_radiologist['name1']+' '+df_radiologist['name2']

        radiologit = df_radiologist[df_radiologist['nam']==nam_r][['ref']]

        if (radiologit.shape[0]==1):
            radiol_ref = radiologit.ref.values[0]
        elif (radiologit.shape[0]==0):
            raise Exception("Sorry, the radiologist code does'nt exist, please check master table.")
        elif (radiologist.shape[0]>1):
            raise TypeError("There are duplicate codes in the master radiologist table, please check.")


        #===========================================================================================
        #====================== Message generation =================================================
        #===========================================================================================

        ORM = core.Message("ORM_O01")

        ##===== MSH - Message Header
        ORM.msh.msh_2 = "^~\&"
        ORM.msh.msh_3 = "QDOC"
        ORM.msh.msh_4 = "HL7V1.1"
        ORM.msh.msh_5 = "AGFA"
        ORM.msh.msh_6 = "AGFA"
        #ORM.msh.msh_7 = "20210227090118" # Date/Time Of Message
        ORM.msh.msh_9.msh_9_1 = "ORM"
        ORM.msh.msh_9.msh_9_2 = "O01"
        ORM.msh.msh_10 = accession_number # Accession Number (V)
        ORM.msh.msh_11 = "P" # Processing ID. P:Production
        ORM.msh.msh_12 = "2.3.1"
        ORM.msh.msh_18 = "8859/1" # Character Set.8859/1:ISO 8859/1

        #===== PID - Patient Identification
        ORM.add_segment("PID")
        ORM.pid.pid_2 = doc_identidad # (v)
        ORM.pid.pid_5.pid_5_1 = nombres[0] # (v)
        ORM.pid.pid_5.pid_5_2 = nombres[1] # (v)
        ORM.pid.pid_7 = birth # Birth (v)
        ORM.pid.pid_8 = sex # Sex (v)

        #===== PV1 - Patient Visit
        ORM.pv1.pv1_2 = 'I' # Patient Class. I: Inpatient

        #===== ORC - Common Order
        ORM.add_segment("ORC")
        ORM.orc.orc_1 = 'XO' # Order Control. XO: Change order/service request
        ORM.orc.orc_3.orc_3_1 = '2141407' # order (v)
        ORM.orc.orc_3.orc_3_2 = sede_id # id_modality (v)
        ORM.orc.orc_12.orc_12_1 = radiol_ref # Ordering Provider. ID of radiologist (v)
        ORM.orc.orc_12.orc_12_2 = surnames_r # radiologist surnames (v)
        ORM.orc.orc_12.orc_12_3 = names_r[0] # radiologist name1 (v)
        ORM.orc.orc_12.orc_12_4 = names_r[1] # radiologist name2 (v)


        #===== OBR - Observation Request
        ORM.add_segment("OBR")
        ORM.obr.obr_3.obr_3_1 = '2141407' # order (v)
        ORM.obr.obr_3.obr_3_2 = sede_id # id_modality (v)
        ORM.obr.obr_4.obr_4_1 = cod_agfa # study code. 0062: agfa code (v)
        ORM.obr.obr_4.obr_4_2 = description # study name. Agfa study name (v)
        ORM.obr.obr_4.obr_4_3 = sede_id # id_modality (v)
        ORM.obr.obr_4.obr_4_6 = "QUADRAT" # ??
        ORM.obr.obr_6 = fecha_estudio # Requested Date. Fecha de recepción de la imágen (v)
        ORM.obr.obr_7 = ORM.msh.msh_7 # Observation Date. Fecha de generación de la predicción (v)
        ORM.obr.obr_16.obr_16_1 = radiol_ref # Ordering Provider. ID of radiologist (v)
        ORM.obr.obr_16.obr_16_2 = surnames_r # radiologist surnames (v)
        ORM.obr.obr_16.obr_16_3 = names_r[0] # radiologist name1 (v)
        ORM.obr.obr_16.obr_16_4 = names_r[1] # radiologist name2 (v)
        ORM.obr.obr_18 = "2141407" # order (v)
        ORM.obr.obr_19 = "2141407" # order (v)
        ORM.obr.obr_20 = "2141407" # order (v)


        #===== OBX - Observation/Result
        ORM.add_segment("OBX")
        ORM.obx.obx_1 = '1' # ID
        ORM.obx.obx_2 = 'TX' # Value type. TX: Text Data
        ORM.obx.obx_3.obx_3_1 = 'ETIQUETA_TEXT' # Observation Identifier
        ORM.obx.obx_3.obx_3_2 = 'OETIQUETA'
        ORM.obx.obx_5 = pred # Modelo prediction


        #===== ZDS
        ORM.add_segment("ZDS")
        ORM.zds.zds_1 = '1.3.51.0.1.1.192.168.10.228.2141407.642336^Agfa^Application^DICOM' # ID


        if(ORM.msh.validate()):
            ORM_message = ORM.value
        else:
            raise Exception('ORM message was not constructed.')


        return(ORM_message)


    def checkConnection(self):
        cliente = socket.socket()
        cliente.settimeout(5)
        try:
            cliente.connect((self.host, self.port))
            check = True
        except:
            check = False

        return(check)



    def sendMessage(self):
        check = self.checkConnection()
        if(check):
            hl7_message = self.buildORM()

            with MLLPClient(self.host, self.port) as client:
                client.send_message(hl7_message)

        else:
            print('There are connection problems for sending the message, please check.')

        with open("logs.log", "a") as f:
            dat = self.readJson() # Dicom header
            accession_number = dat['00080050']['Value'][0]
            fecha_estudio = dat['00080022']['Value'][0]+dat['00080032']['Value'][0].split('.')[0]
            doc_identidad = dat['00100020']['Value'][0]
            lin_log = [self.host,fecha_estudio,accession_number,doc_identidad,str(self.prediction),str(check)]
            line_log = "|".join(lin_log)
            f.write(line_log+'\n')
        return(check)

class storedb:

    def __init__(self,path_dicom_header,prediction):

        self.path_dicom_header = path_dicom_header
        self.prediction = prediction

    def readJson(self):
        '''
        Read a json file into a dictionary
        inputs:
            -path_dicom_header:path of json file with dicom header
        output:
            - output: dictionary objet

        Example:
                prueba = builAndSedHL7('test_info_dicom_xnat.json',0,'agfa_sap_code.xlsx','radiologist.xlsx','172.25.121.208',8084)
                prueba.buildORM() # constructed message
                prueba.sendMessage() # Sending message
        '''
        #Read dicom header
        with open(self.path_dicom_header) as json_file:
            header_dicom = json.load(json_file)

        return(header_dicom)

    def getdata(self):
        '''
        Build a ORM hl7 message
        input:
            - path_dicom_header: Header of DICOM image (json)
            - prediction: Prediction of model (0: healty, 1: pathologic)
        '''

        dat = self.readJson() # Dicom header

        doc_identidad = dat['00100020']['Value'][0]
        accession_number = dat['00080050']['Value'][0]
        fecha_estudio = dat['00080022']['Value'][0]+dat['00080032']['Value'][0].split('.')[0]
        fecha_estudio_datetime_obj=datetime.strptime(fecha_estudio,'%Y%m%d%H%M%S')

        if(self.prediction==0):
            pred = 'SANO'
        elif(self.prediction==1):
            pred = 'PATOLOGICO'
        else:
            raise TypeError('Prediction value must be 0 or 1.')

        return fecha_estudio_datetime_obj, accession_number, doc_identidad

#Execute ML model from files located in ${BASEDIR}
y_pred = getDataFromDirectory_and_predict_production(sys.argv[2]) # sys.argv[2] for the following instruction python main.py ${INPUTDIR} ${BASEDIR}
print(sys.argv[2])
# load metadata from DICOM files
description_json=str(sys.argv[2])+'/description.json' #Second entry correponds to .json file

#===========================================================================================
#====================== Send to Database  ==================================================
#===========================================================================================

senddb=storedb(description_json,y_pred) #Read information from json
values=senddb.getdata() # save date, accession_number and patient ID in "values"

# initialize list of lists
data = [[datetime.strftime(datetime.date(values[0]),'%Y%m%d'), values[1], values[2], int(y_pred)]] #Changes the format of the date

# Create the pandas DataFrame
df = pd.DataFrame(data, columns = ['date', 'access_number', 'patient_id', 'prediction_triaje']) # Dataframe with the information to bbe saved in the db

connection_uri = 'postgresql://APPTABLEROSVF:app!SVF*2020@172.28.24.108:5432/APPTABLEROSVF'
engine = create_engine(connection_uri)

# Rename columns
metadata = MetaData()
table_obj = Table('clasificacion_triaje_RM', metadata, autoload=True, autoload_with=engine)
columns = [col.name for col in table_obj.columns]
df.columns = columns

# Load DataFrame into Postgres database
df.to_sql(
    name='clasificacion_triaje_RM',
    con=engine,
    if_exists='append',
    index=False)

#===========================================================================================
#====================== Send Message  ======================================================
#===========================================================================================

#prueba=builAndSedHL7(description_json,y_pred,'agfa_sap_code.xlsx','radiologist.xlsx','172.25.121.208',8084)
#prueba.buildORM() # constructed message
#prueba.sendMessage() # Sending message
