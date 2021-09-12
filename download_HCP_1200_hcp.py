# download_HCP_1200.py

'''
This script downloads data from the Human Connectome Project - 1200 subjects release.
'''

# Import packages
from loguru import logger
from pathlib import Path
import threading
import boto3
import time
from utils.conf import *
from utils.util import *


project_path = Path.cwd()
log_path = Path(project_path, "log")
out_dir = './data/HPC/'
# out_dir = '/sharing01/sharedata_HCP/'
t = time.strftime("%Y_%m_%d")


# Main collect and download function
@logger.catch()
def collect_and_download(out_dir,subjects,bucket):
    # resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    # bucket = resource.Bucket(s3_bucket_name)
    # print('connect to client successfully!')
    # logger.info("connect to client successfully!")

    for subject in subjects:
        logger.add(f'{log_path}/{subject}_info_{t}.log', rotation="500MB", encoding="utf-8", enqueue=True, compression="zip",
                   retention="10 days", level="INFO")
        logger.add(f'{log_path}/{subject}_error_{t}.log', rotation="500MB", encoding="utf-8", enqueue=True, compression="zip",
                   retention="10 days",level="ERROR")

        time_start = time.time()

        # read md5 file
        fp = open('./data/md5/'+ str(subject) + '_md5.json', 'r', encoding='utf-8')
        subject_md5 = json.loads(fp.readline())['Include']
        fp.close()

        s3_keys = bucket.objects.filter(Prefix='HCP_1200/%s/'%subject)
        s3_keylist = [key.key for key in s3_keys]

        prefixes = ["HCP_1200/%s/%s"%(subject,x) for x in SERIES_MAP.values()]
        prefixes = tuple(prefixes)
        s3_keylist = [x for x in s3_keylist if x.startswith(prefixes)]

        # remove png and html
        # s3_keylist = [x for x in s3_keylist if not x.endswith(('png','html'))]

        # If output path doesn't exist, create it
        if not os.path.exists(out_dir):
            print('Could not find %s, creating now...' % out_dir)
            logger.warning(f'Could not find {out_dir}, creating now...')
            os.makedirs(out_dir)

        total_num_files = len(s3_keylist)
        files_downloaded = len(s3_keylist)

        count = 0

        for path_idx, s3_path in enumerate(s3_keylist):
            count += 1
            rel_path = s3_path.replace(s3_prefix, '')
            rel_path = rel_path.lstrip('/')

            download_file = os.path.join(out_dir, rel_path)
            download_dir = os.path.dirname(download_file)
            # If downloaded file's directory doesn't exist, create it
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            try:
                if not os.path.exists(download_file) or os.path.getsize(download_file) == 0:
                    # while file is empty
                    if os.path.exists(download_file):
                        if os.path.getsize(download_file) == 0:
                            print("%s is empty, download again!" % (s3_path))
                            logger.error(f"{s3_path} is empty, download again!")
                    print('Downloading to: %s' % download_file)
                    # with open(download_file, 'wb') as f:
                    #     # download_file:  The path to the file to download to.
                    #     # s3_path: The name of the key to download from.
                    #     bucket.download_file(s3_path,download_file)
                    bucket.download_file(s3_path,download_file)

                    # md5 integrity verify, Waiting for optimization
                    md5_old = None
                    for item in subject_md5:
                        if rel_path == item['URI']:
                            md5_old = item['Checksum']
                    if md5_old:
                        verify = check_integrity(download_file,md5_old)
                        if not verify:
                            print("Download fail about file: %s"%(s3_path))
                            logger.error(f"Download fail about file: {s3_path}")
                            # download fail, write to a file
                            fw2 = open('./data/fail/'+str(subject)+'_fail.txt','a',encoding='utf-8')
                            fw2.write(s3_path + '\n')
                            fw2.close()
                    else:
                        print("There are not have md5 code about the file: %s" % (s3_path))
                        logger.error(f"There are have not md5 code about the file: {s3_path}")

                    print("FACTS: path: %s, file: %s"%(s3_path, download_file))
                    print('%.3f%% percent complete' % \
                          (100*(float(path_idx+1)/total_num_files)))
                    complete_percent = (100*(float(path_idx+1)/total_num_files))
                    if count%500 == 0:
                        logger.info(f"{complete_percent} percent complete")
                else:
                    print('File %s already exists, skipping...' % download_file)
                    files_downloaded -= 1
            except Exception as exc:
                print('There was a problem downloading %s.\n'\
                      'Check and try again.' % s3_path)
                logger.error(f"There was a problem downloading {s3_path}. Check and try again.")
                print(exc)
                logger.error(exc)

        print('%d files downloaded for subject %s.' % (files_downloaded,subject))
        logger.info(f"{files_downloaded} files downloaded for subject {subject}.")

        print('Done!')
        logger.info("DOne!")

        time_cost = (time.time()-time_start)/3600
        print("Time cost of downloading {} is :{} h".format(subject,time_cost))
        logger.info(f"Time cost of downloading {subject} is :{time_cost} h")


if __name__ == '__main__':
    resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket = resource.Bucket(s3_bucket_name)
    print('connect to client successfully!')
    # logger.info("connect to client successfully!")

    task_subject = divide_subject("./utils/subjects_hcp.txt", 4)

    # subjects = [101107,911849,995174]

    # collect_and_download(out_dir=out_dir,subjects=subjects,bucket=bucket)

    a1 = threading.Thread(target=collect_and_download,args=(out_dir,task_subject[0],bucket))
    a2 = threading.Thread(target=collect_and_download,args=(out_dir,task_subject[1],bucket))
    a3 = threading.Thread(target=collect_and_download,args=(out_dir,task_subject[2],bucket))
    a4 = threading.Thread(target=collect_and_download,args=(out_dir,task_subject[3],bucket))
    # #
    a1.start()
    time.sleep(40)
    a2.start()
    time.sleep(40)
    a3.start()
    time.sleep(40)
    a4.start()

