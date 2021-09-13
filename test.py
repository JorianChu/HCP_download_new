# encoding = utf-8
"""
@version:0.1
@author: jorian
@time: 2021/9/9  17:35
"""
import boto3
import json

# access_key = ''
# secret_key = ''
# subject = '911849'
#
# resource = boto3.resource('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
# bucket = resource.Bucket('hcp-openaccess')
# # for bucket in resource.buckets.all():
# #     print(bucket.name)
#
# s3_keys = bucket.objects.filter(Prefix='HCP_1200/%s/'%subject)
# s3_keylist = [key.key for key in s3_keys]

# data1 = {"name":"jorian","height":"170"}
# data2 = {"name":"aaron","height":"170"}
#
# lis1 = [data1]
# lis2 = [data2]
# # data = data1 + data2
# lis = lis1 + lis2
#
# lis_new = lis1
# lis_new.extend(lis2)
# lis3 = []
# lis3.append(lis1)
# lis3.append(lis2)
# # print(lis_new)
#
# # print(data)
# print(lis)
# print(lis_new)
# print(lis3)