#!/bin/bash

Usage() {
    cat <<EOF

dicom_description.sh is designed to extract metadata from DICOM files.

To run it you must have installed DCMTK in your computer, it requires the function dcm2json.

Usage: dicom_description.sh <input_folder>
inside the imput folder it must be DICOM images.

e.g.   dicom_description.sh /Users/neurofuncional/Documents/Test_Triage_XNAT/Galeano_Marin_Marilin_Manuela/eFL_AX_1002

EOF
    exit 1
}

baseDIR="$1"
[ "$1" = "" ] && Usage

files=($(ls ${baseDIR}/*.dcm))
#files=(`ls ${baseDIR}/*.dcm`)
#echo "${files[0]}"

dcm2json ${files[0]} ${baseDIR}/description.json #quitar ${baseDIR} si se está trabajando sobre el directorio de las imagenes
