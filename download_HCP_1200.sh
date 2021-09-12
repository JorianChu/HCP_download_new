#!/bin/bash
#SBATCH --job-name=HCP_download
#SBATCH --time=4:00:00

#module load system
#module load py-scipystack/1.0_py27

#SUBJECT=$(python utils/get_subject.py 2>&1)
#SUBJECT = $(cat utils/subjects.txt)

#python download_HCP_1200.py --subject=$SUBJECT --out_dir=/sharing01/sharedata_HCP/ --tartasks
#for line in `cat utils/subjects_test.txt`
#do
#  echo $line
#  python download_HCP_1200.py --subject=$line --out_dir=/sharing01/sharedata_HCP/
#done

cat utils/subjects_hcp.txt | while read line;


#do
#{
#  echo $line;
##  python download_HCP_1200.py --subject=$line --out_dir=/sharing01/sharedata_HCP/;
#} &
#done

# start 7 thread, execute download task parallely
#for i in {1..7}
#do
#{
#  echo $line;
#  do
#    echo
#  done
#} &
#done

#sleep 60