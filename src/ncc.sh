#!/bin/bash
Usage() {
    cat <<EOF

NCC Normalized cross correlation between two volumes
Developed by gABoCas 16.02.2016 based on fsl mail list post
(https://www.jiscmail.ac.uk/cgi-bin/webadmin?A2=fsl;1057accd.1202)


Usage: ncc file1 file2 mask

e.g.   mr_triage file1_brain file2_brain mask

EOF
    exit 1
}

[ "$1" = "" ] && Usage

ncc_fn=$1; shift
img1=$1; shift
img2=$1; shift
mask=$1; shift
out_dir=`dirname $img1`

# mean
M1=`fslstats $img1 -k $mask -m`
M2=`fslstats $img2 -k $mask -m`

# std
S1=`fslstats $img1 -k $mask -s`
S2=`fslstats $img2 -k $mask -s`

fslmaths $img1 -sub $M1 -mas $mask ${out_dir}/demeaned1 -odt float
fslmaths $img2 -sub $M2 -mas $mask ${out_dir}/demeaned2 -odt float
fslmaths ${out_dir}/demeaned1 -mul ${out_dir}/demeaned2 -div $S1 -div $S2 -abs -fmedian ${out_dir}/${ncc_fn}

# Numerator of NCC
ncc=`fslstats ${out_dir}/${ncc_fn} -k $mask -m`
rm -r ${out_dir}/demeaned*
[[ ! -z "$1" ]] && echo $ncc
