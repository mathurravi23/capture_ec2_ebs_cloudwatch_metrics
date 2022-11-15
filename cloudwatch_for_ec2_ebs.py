import time
import numpy as np
import datetime
import csv
import boto3
from botocore.exceptions import ClientError
import argparse
from datetime import datetime, timedelta
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description='Python script to collect Amazon CloudWatch metrics for Amazon EC2 instances & associated EBS volumes')
    parser.add_argument('-i', '--input_file', help='input_file', type=str, required=False)
    parser.add_argument('-o', '--output_file', help='output_file', type=str, required=False)
    parser.add_argument('-r', '--region', help='AWS Region', type=str, required=False)
    parser.add_argument('-d', '--days_back', help='days_back', type=int, required=False)
    parser.set_defaults(input_file='noinput', output_file='ebs-ec2-output.csv', period=300,days_back=30,region='us-east-1')
    args = parser.parse_args()
    return args


def divide_numbers(x,y):
    np.seterr(invalid='ignore')
    try:
        val = x / y
        if np.isnan(val):
            return 0
        else:
            return val
    except:
        return 0

# to determine the average
def calc_avg_iop(row_dict):
    # divide monthly throughput by monthly IOPS per month
    row_dict['VolumeOpsSum'] = row_dict['VolumeReadOpsSum'] + row_dict['VolumeWriteOpsSum']
    row_dict['VolumeBytesSum'] = row_dict['VolumeReadBytesSum'] + row_dict['VolumeWriteBytesSum']
    row_dict['IoSize'] = divide_numbers(row_dict['VolumeBytesSum'], row_dict['VolumeOpsSum'])
    return row_dict


def get_ebs_metrics(cw_client,vol_id,metric_name,stat,unit,days_back,period):
  df = pd.DataFrame()
  cw_response = cw_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'string1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/EBS',
                        'MetricName': metric_name,
                        'Dimensions': [
                            {
                                'Name': 'VolumeId',
                                'Value': vol_id
                            },
                        ]
                    },
                    'Period': period,
                    'Stat': stat,
                    'Unit': unit
                },
                'ReturnData': True
            },
        ],
        StartTime=datetime.utcnow() - timedelta(days=days_back),
        EndTime=datetime.utcnow(),

    )
  df[metric_name] = cw_response['MetricDataResults'][0]['Values']
  return df

def get_ec2_metrics(cw,resource_id,metric_name,stat,unit,days_back,period):
    datapoints = {}
    result = cw.get_metric_data(
       MetricDataQueries=[
          {
              'Id': 'ec2data',
              'MetricStat': {
                  'Metric': {
                      'Namespace': 'AWS/EC2',
                      'MetricName': metric_name,
                      'Dimensions': [
                          {
                              'Name': 'InstanceId',
                              'Value': resource_id
                          },
                      ]
                  },
                  'Period': period,
                  'Stat': stat,
                  'Unit': unit
              },
              'ReturnData': True
          },
        ],

        StartTime=datetime.utcnow() - timedelta(days=days_back),
        EndTime=datetime.utcnow()
    )
    datapoints[metric_name] = result['MetricDataResults'][0]['Values']

    return datapoints


def main():
    args = parse_args()
    output_df = pd.DataFrame()
    session = boto3.session.Session(region_name=args.region)
    cw = session.client('cloudwatch')
    ec2_client=session.client('ec2')

    ebs_metrics = {
        'VolumeReadOps': 'Count',
        'VolumeWriteOps': 'Count',
        'VolumeReadBytes': 'Bytes',
        'VolumeWriteBytes': 'Bytes'
    }

    ebs_stat = ['Maximum', 'Sum']

    ec2_metrics = {
        'CPUUtilization': 'Percent',
      }

    # days back period to poll cloudwatch
    days_back = args.days_back
    input_file = args.input_file
    month_span = days_back/30
    output_file = args.output_file

    print('#############################################')
    print('AWS Region:',args.region)
    print('Input File:',args.input_file)
    print('Output File:',args.output_file)
    print('Metric history length(days_back):',args.days_back)
    print('#############################################')


    #Creating list of EC2 instances in-scope. This list is either supplied as input or script capture all running EC2 instances within the AWS region.

    ec2_list = []
    if input_file == 'noinput':
       ec2_resources = ec2_client.describe_instances(Filters=[ {'Name': 'instance-state-name', 'Values': ['running']}])
       for i in range(len(ec2_resources['Reservations'])):
           ec2_list.append(ec2_resources['Reservations'][i]['Instances'][0]['InstanceId'])
    else:
       with open(args.input_file,'r') as file:
          ec2_resources = csv.reader(file)
          for resource in ec2_resources:
             ec2_list.append(resource[0])
    if len(ec2_list) <1 :
       print('EC2 Instance List is empty or no instance found!!')
    else:
        col_list = list(output_df.columns)
        output_df.to_csv(output_file, index=False, columns=(sorted(col_list, reverse=True)))
        for instance in ec2_list:
            print('...Now collecting metrics for EC2 box:',instance)
            try:
                row_dict = {}

                instance_details = ec2_client.describe_instances(InstanceIds=[instance])
                tag_list = instance_details['Reservations'][0]['Instances'][0]['Tags']
                inst_nm = ''
                inst_nm = [nm for nm in tag_list if nm['Key'] == 'Name']
                if inst_nm !='':
                    row_dict['Instance_Name'] = inst_nm[0]['Value']
                else:
                    row_dict['Instance_Name'] = ''
                row_dict['Instance_Id'] = instance_details['Reservations'][0]['Instances'][0]['InstanceId']
                row_dict['Instance_Type'] = instance_details['Reservations'][0]['Instances'][0]['InstanceType']
                row_dict['Platform'] = instance_details['Reservations'][0]['Instances'][0]['PlatformDetails']
                row_dict['EbsOptimized'] = instance_details['Reservations'][0]['Instances'][0]['EbsOptimized']
                row_dict['RootDeviceName'] = instance_details['Reservations'][0]['Instances'][0]['RootDeviceName']
                row_dict['RootDeviceType'] = instance_details['Reservations'][0]['Instances'][0]['RootDeviceType']

                for metric_name,unit in ec2_metrics.items():
                    ec2_metrics_mx = get_ec2_metrics(cw,instance,metric_name,'Maximum',unit,days_back,300)
                    row_dict[metric_name+'_Max'] = max(ec2_metrics_mx[metric_name])
                    ec2_metrics_avg = get_ec2_metrics(cw,instance,metric_name,'Average',unit,days_back,300)
                    row_dict[metric_name+'_Avg'] = np.average(ec2_metrics_avg[metric_name])

                #Generating list of EBS volumes attached to each ec2 instance
                vol_cnt = len(instance_details['Reservations'][0]['Instances'][0]['BlockDeviceMappings'])

                for j in range(vol_cnt):
                    vol_id = instance_details['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][j]['Ebs']['VolumeId']

                    print('......collecting metrics for volume:',vol_id)

                    vol_info = ec2_client.describe_volumes(VolumeIds=[vol_id])
                    row_dict['Volume_Name'] = ''
                    #Exception block to handle empty tag list, for volumes without any tags
                    try:
                        vol_tag_list = vol_info['Volumes'][0]['Tags']
                        vol_nm = ''
                        vol_nm = [vnm for vnm in vol_tag_list if vnm['Key'] == 'Name']
                        if vol_nm !='':
                            row_dict['Volume_Name'] = vol_nm[0]['Value']
                    except Exception:
                        pass
                    row_dict['Volume_Id'] = vol_id
                    row_dict['Volume_Type'] = vol_info['Volumes'][0]['VolumeType']
                    row_dict['Volume_Device'] = vol_info['Volumes'][0]['Attachments'][0]['Device']
                    row_dict['Volume_state'] = vol_info['Volumes'][0]['State']
                    row_dict['Volume_Allocated_Size (GiB)'] = vol_info['Volumes'][0]['Size']
                    row_dict['Volume_Provision_IOPS'] = vol_info['Volumes'][0]['Iops']
                    row_dict['Volume_Encrypted'] = vol_info['Volumes'][0]['Encrypted']

                    #Generating EBS metrics per volume and writing to csv
                    for stat in ebs_stat:
                        for metric_name,unit in ebs_metrics.items():
                            try:
                                time.sleep(2)
                                df = pd.DataFrame()
                                df = get_ebs_metrics(cw,vol_id,metric_name,stat,unit,days_back,300)
                                # divide by 60 seconds 1 hertz data for a 60 second period

                                if stat == 'Maximum':
                                    df_max = df.div(60)
                                    df_max = df_max.round(1)
                                    max_value = df_max[metric_name].max()
                                    row_dict[metric_name + 'Maximum'] = max_value
                             # only get Sum for throughtput stats
                                if stat == 'Sum' and (metric_name == 'VolumeReadBytes' or 'VolumeWriteBytes' or 'VolumeReadOps' or 'VolumeWriteOps'):
                                    row_dict[metric_name + 'Sum'] = (df[metric_name].sum()/month_span)
                            except Exception as e:
                                print(f'An error occurred during making call for EBS id: {vol_id}, metric: {metric_name}')
                                print(e)
                                pass

                    row_dict = calc_avg_iop(row_dict)
                    # round off decimal values
                    df_temp = pd.DataFrame(row_dict, index=[0]).round(0)
                    #print(f'Query result: {df_temp}')
                    output_df = pd.concat([output_df, df_temp])
            #get dataframe column list for ordering csv columns
                col_list = list(output_df.columns)
                output_df.to_csv(output_file, index=False, columns=(col_list))
            except Exception as e:
                print(f'An error occurred during making call for EC2 instance: {instance}')
                print(e)
                pass
    print(f'Output file generated as',output_file)


if __name__ == "__main__":
    main()
