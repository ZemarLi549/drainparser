from logparser import Drain
import os
import csv
PULL_LOG_DIR = '/data/logparser/'
OUT_CSV_DIR = '/data/Drain_result/'

def Drain_parcer(log_file):
    input_dir = PULL_LOG_DIR  # The input directory of log file
    output_dir = OUT_CSV_DIR  # The output directory of parsing results
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # log_file   = 'HDFS_2k.log'  # The input log file name
    log_format = '\[<Date>\] \[<Level>\] \[<Component>\] \[<Thread>\].*\[-<Content>-\].*'  # HDFS log format
    # #自定义初始化转* regex
    regex = [
        r'userId_(|-)[0-9]+',  # block id
        r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)',  # IP
        r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$',  # Numbers
        #date
        r'(((\d\d)(([02468][048])|([13579][26]))-02-29)|(((\d\d)(\d\d)))-((((0\d)|(1[0-2]))-((0\d)|(1\d)|(2[0-8])))|((((0[13578])|(1[02]))-31)|(((0[1,3-9])|(1[0-2]))-(29|30)))))\s(([01]\d|2[0-3]):([0-5]\d):([0-5]\d))'
    ]
    st = 0.5  # 相似度阈值,单词列表的相同个数/原列表
    depth = 4  # 定义子节点深度
    parser = Drain.LogParser(log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex)
    parser.parse(log_file)

if __name__ == '__main__':
    ls_str = []
    fileName = '/tmp/pejala_errrorlog_20220822'#自定义修改
    with open(f'{OUT_CSV_DIR}{fileName}_templates.csv', 'r') as f:
        csvreader = csv.reader(f)
        a = next(csvreader)
        i = 0
        for row in csvreader:
            i += 1
            EventTemplate = row[1]
            Occurrences = row[2]
            ls_str.append({'ET': EventTemplate, 'CT': Occurrences})
    if ls_str:
        ls_str.sort(key=lambda x: x['CT'], reverse=True)
    print(ls_str)