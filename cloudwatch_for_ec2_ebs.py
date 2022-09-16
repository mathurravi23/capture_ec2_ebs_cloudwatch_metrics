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
    parser = argparse.ArgumentParser(description='instance check script')
    parser.add_argument('-i', '--input_file', help='input_file', type=str, required=False)
    parser.add_argument('-o', '--output_file', help='output_file', type=str, required=False)
    parser.add_argument('-r', '--region', help='AWS Region', type=str, required=False)
    parser.add_argument("-p", "--profile", help="The credential profile to use if not using default credentials")
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
   # now = datetime.datetime.now()
    #for metric_name,unit in ec2_metrics.items():
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
    ec2 = session.resource('ec2')

    csv_headers = [
                'InstanceName',
                'Instance Id',
                'Instance Type',
                'Hypervisor',
                'Virtualization Type',
                'Architecture',
                'EBS Optimized',
                'Max CPU %',
                'Avg CPU %',
                'EBS Volume',
                'VolumeReadOpsSum',
                'VolumeWriteOpsSum',
                'VolumeReadBytesSum',
                'VolumeWriteBytesSum'
            ]

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


    #Creating list of EC2 instances in-scope. This list is either supplied as input or script capture all running EC2 instances within the AWS region.

    ec2_list = []
    if input_file == 'noinput':
       ec2_resources = ec2.instances.filter(Filters=[ {'Name': 'instance-state-name', 'Values': ['running']}])
       for resource in ec2_resources:
          ec2_list.append(resource.id)
    else:
       with open(args.input_file,'r') as file:
          ec2_resources = csv.reader(file)
          for resource in ec2_resources:
             ec2_list.append(resource[0])
    if len(ec2_list) <1 :
       print('EC2 Instance List is empty...')
    else:
        col_list = list(output_df.columns)
        output_df.to_csv(output_file, index=False, columns=(sorted(col_list, reverse=True)))
        for instance in ec2_list:
            print('...Now collecting metrics for EC2 box:',instance)
            try:
                row_dict = {}
                for metric_name,unit in ec2_metrics.items():
                    ec2_metrics_mx = get_ec2_metrics(cw,instance,metric_name,'Maximum',unit,days_back,300)
                    row_dict[metric_name+'_Max'] = max(ec2_metrics_mx[metric_name])
                    ec2_metrics_avg = get_ec2_metrics(cw,instance,metric_name,'Average',unit,days_back,300)
                    row_dict[metric_name+'_Avg'] = np.average(ec2_metrics_avg[metric_name])
                #Generating list of EBS volumes attached to each ec2 instance
                instance_ids =ec2.Instance(instance)
                row_dict['Instance Type'] = instance_ids.instance_type
                row_dict['Platform'] = instance_ids.platform
                ebs_volumes = instance_ids.volumes.all()
                for vol in ebs_volumes:
                    #Generating EBS metrics per volume and writing to csv
                    for stat in ebs_stat:
                        for metric_name,unit in ebs_metrics.items():
                            try:
                                time.sleep(2)
                                df = pd.DataFrame()
                                df = get_ebs_metrics(cw,vol.id,metric_name,stat,unit,days_back,300)
                                # divide by 60 seconds 1 hertz data for a 60 second period

                                if stat == 'Maximum':
                                    df_max = df.div(60)
                                    df_max = df_max.round(1)
                                    max_value = df_max[metric_name].max()
                                    row_dict[metric_name + 'Maximum'] = max_value
                             # only get Sum for throughtput stats
                                if stat == 'Sum' and (metric_name == 'VolumeReadBytes' or 'VolumeWriteBytes' or 'VolumeReadOps' or 'VolumeWriteOps'):
                                    row_dict[metric_name + 'Sum'] = (df[metric_name].sum()/month_span)
                                row_dict['instance_id'] = instance
                                row_dict['ebs_vol_id'] = vol.id
                            except Exception as e:
                                print(f'An error occurred during making call for EBS id: {vol.id}, metric: {metric_name}')
                                print(e)
                                pass

                    row_dict = calc_avg_iop(row_dict)
                    # round off decimal values
                    df_temp = pd.DataFrame(row_dict, index=[0]).round(0)
                    print(f'Query result: {df_temp}')
                    output_df = pd.concat([output_df, df_temp])

            #get dataframe column list for ordering csv columns
                col_list = list(output_df.columns)
                output_df.to_csv(output_file, index=False, columns=(sorted(col_list, reverse=True)))

            except Exception as e:
                print(f'An error occurred during making call for EC2 instance: {instance}')
                print(e)
                pass


if __name__ == "__main__":
    main()
