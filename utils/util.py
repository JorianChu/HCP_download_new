# encoding = utf-8
"""
@version:0.1
@author: jorian
@time: 2021/9/10  22:28
"""
import hashlib
import os
import json


def get_file_md5(file):
    if not os.path.isfile(file):
        # return
        raise Exception("The file:%s is not exist! Can't get md5 code!"%file)
    m = hashlib.md5()
    with open(file, mode='rb') as f:
        while True:
            data = f.read(10240)
            if not data:
                break
            m.update(data)
    return m.hexdigest()


def check_integrity(file,md5_old):
    md5 = get_file_md5(file)
    if md5 == md5_old:
        return True
    return False


def divide_subject(subject_path, divide_num):
    subject_list = []
    fr = open(subject_path, 'r', encoding='utf-8')
    line = fr.readline()
    while line:
        subject_list.append(line.strip())
        line = fr.readline()

    task_subject = []
    size = int(len(subject_list) /divide_num)
    flag = False
    if len(subject_list) % divide_num == 0:
        for i in range(divide_num):
            task_subject.append(subject_list[size * i:size * (i + 1)])
    else:
        for i in range(divide_num + 1):
            task_subject.append(subject_list[size * i:size * (i + 1)])
        flag = True
    if flag:
        task_subject[-2] += task_subject[-1]
        task_subject.pop(-1)

    return subject_list


def merge_subject_json(subject, root_dir, out_dir):
    out_file = os.path.join(out_dir,str(subject)+'_md5.json')
    paths = os.listdir(root_dir)
    count = 0  # 记录数据总条数

    merged_data = []
    for path in paths:
        file_path = os.path.join(root_dir, path)
        print(file_path)
        sds_name, suffix = os.path.splitext(path)
        if suffix != '.json':
            continue

        fp = open(file_path, 'r', encoding='utf-8')
        data = json.loads(fp.readline())
        data = data['DownloadManifest']['Includes']
        # attention: the dic content can't repeat
        merged_data.extend(data)
        # !!!!!        merged_data += data
        fp.close()
    fw = open(out_file, 'w', encoding='utf-8')
    subject_md5 = {"subject":subject, "Include":merged_data}
    json.dump(subject_md5, fw, ensure_ascii=False)
    fw.close()


def merge_subject_json2(subject, root_dir, out_dir):
    out_file = os.path.join(out_dir,str(subject)+'_md5.json')
    paths = os.listdir(root_dir)
    count = 0  # 记录数据总条数
    with open(out_file, "wb") as f:
        for path in paths:
            file_path = os.path.join(root_dir, path)
            print(file_path)
            sds_name, suffix = os.path.splitext(path)
            if (suffix != '.json'):
                continue
            with open(file_path, 'r', encoding='utf8')as fp:
                for line in fp:
                    data = json.loads(line)  # 字典类型
                    count = count + 1
                    # print(data)
                    # print('这是读取到文件数据的数据类型：', type(data))
                    print(count)
                    json.dump(data, f, ensure_ascii=False)  # 写入文件，ensure_ascii=False避免将中文转化为编码
                    f.write('\r\n')
                    # print(file_path + "加载入文件完成...")

# merge_subject_json(101107,'../data/.xdlm/101107/','../data/md5/')

def get_subject_file_nums(md5_file_path):
    fp = open(md5_file_path, 'r', encoding='utf-8')
    lis = json.loads(fp.readline())['Include']
    fp.close()
    # count = 0
    # for data in lis:
    #     count += len(data)
    print(len(lis))
# get_subject_file_nums('../data/md5/101107_md5.json')  #101107: 11614

