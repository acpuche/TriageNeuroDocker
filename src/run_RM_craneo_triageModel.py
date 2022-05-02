#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import json
from joblib import load
from nilearn.image import get_data
import sys


def getDataFromDirectory_and_predict_production(directory):
    """
    Function that predicts whether a skull MRI is healthy or pathological

    input:
        path_dicom: path of dicom image(s)
        master: master.csv with accesion_number column#

    output: Array with prediction where file: healthy and 1: pathlogic

    Note: This file and Eigenfaces_subespace.joblib, SGB_NCC.joblib and scaler.joblib  files,
    must be in the same folder.

    example: getDataFromDirectory_and_predict_production('/dicom/',df.path.values)
    df is a master.csv dataframe with accesion number column

    """

    # Ruta de interés para lectura del DICOM:
    #/data/xnat/archive/eFL_AX/arc001/AssesionNumber/SCANS/*/DICOM

    # Load data model
    heat_pca = load('Eigenfaces_subespace.joblib')
    SGB = load('SGB_NCC_corrected.joblib')
    scaler = load('scaler.joblib')

    # read data from image(s)
    data=[]

    x = get_data(directory + '/NCC_map_nt.nii.gz')
    dim = x.shape

    if(dim[0]!=91 or dim[1]!=109 or dim[2]!=91):
        sys.exit('The dimensions of the correlation map should be 91*109*91')

    x = x.reshape(dim[0]*dim[1]*dim[2])
    data.append(x)

    # Prection
    projection = heat_pca.transform(data)
    #X_data = scaler.transform(projection)
    y_pred = SGB.predict(projection) # (0: healthy, 1: pathlogic)

    print(y_pred)

    return y_pred


#getDataFromDirectory_and_predict_production(sys.argv[1])
