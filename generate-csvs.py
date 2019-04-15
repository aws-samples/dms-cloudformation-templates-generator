import argparse, os, csv, re, datetime

source_arns = [
   "arn:aws:dms:ap-south-1:951590926382:endpoint:TLYBTFPQXZANQTAUERKQBFHPSY"
]
target_arns = ["arn:aws:dms:ap-south-1:951590926382:endpoint:OR36TVWAZEBFOIFSIUTWKUCYLU"]
rep_arns = [
    "arn:aws:dms:ap-south-1:951590926382:rep:KT3BCIWZAIHTKBB437VSV7VYZI"
]

csv.register_dialect('spaceSplit',delimiter = ' ',skipinitialspace=True)

fields = ['Sl no','TaskName','TaskDescription','SchemaName','TableName',
          'ExcludeColumns','FilterColumn','FilterCondition','StartValue',
          'EndValue','taskPrepMode','maxSubTasks','lobMode','fullLob',
          'chunkSize','validation','sourceARN','targetARN','repARN','renameTable', 'BatchApplyEnabled']


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def process(path, lobMode,
            lobSize, rename_to=None):
    if not os.path.exists(os.path.join(path, "output")): # Creating output folder if not exists
        os.makedirs(os.path.join(path, "output"))
    files = (file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)))
    for f in files:
        schema_name, table_name = f.split('.')[0], f.split('.')[1]
        key_column = ''
        rows = list()
        valid_rows = list()
        with open(os.path.join(path,f), 'r') as inputFile:
            reader = csv.reader(inputFile, dialect='spaceSplit')
            i = 1
            skip_count = 0
            raw_rows = list(reader)
            for row in raw_rows:
                if len(row) == 0 or any('--' in r for r in row):
                    skip_count += 1
                elif any('MIN' in r for r in row) and any('MAX' in r for r in row):
                    key_column = re.findall( r'\((\w+)\)',row[0])[0]
                    skip_count += 1
                elif (not row[0].lstrip().isdigit() or not row[1].lstrip().isdigit()):
                    skip_count += 1
                else:
                    valid_rows.append(row)
            end = None
            for row in valid_rows:
                res = {
                    'SchemaName':schema_name,
                    'TableName':table_name,
                    'FilterColumn':key_column,
                    'taskPrepMode': 'DO_NOTHING',#'TRUNCATE_BEFORE_LOAD' if i == 1 else 'DO_NOTHING',
                    'maxSubTasks' : 8,
                    'sourceARN': source_arns[i%len(source_arns)],
                    'targetARN': target_arns[i%len(target_arns)],
                    'repARN': rep_arns[(i%len(rep_arns))]
                    }
                res['TaskName'] = '%s-%s-rep-%s-s-%s-%s'%(table_name, i, (i%len(rep_arns)) + 1, (i%len(source_arns))+1, str(datetime.date.today()))
                res['FilterCondition'] = 'between'
                
                if i == 1: res['FilterCondition'] = 'ste'
                elif i == len(valid_rows):
                    res_final = {
                        'SchemaName':schema_name,
                        'TableName':table_name,
                        'FilterColumn':key_column,
                        'taskPrepMode': 'TRUNCATE_BEFORE_LOAD' if i == 1 else 'DO_NOTHING',
                        'maxSubTasks' : 8,
                        'sourceARN': source_arns[i%len(source_arns)],
                        'targetARN': target_arns[i%len(target_arns)],
                        'repARN': rep_arns[(i%len(rep_arns))]
                    }
                    res_final['TaskName'] = '%s-%s-rep-%s-s-%s-%s'%(table_name, i+1, (i+1%len(rep_arns)) + 1, (i+1%len(source_arns))+1, str(datetime.date.today()))
                    res_final['FilterCondition'] = 'gte'
                    res_final['StartValue'] = row[1]
                    if rename_to:
                         res_final["renameTable"] = rename_to
                    if lobMode == 'Y':
                        res_final['lobMode'] = 'TRUE'
                        res_final['chunkSize'] = lobSize
                    rows.append(res_final)
                    
                res['TaskName'] = '%s-%s-rep-%s-s-%s-%s'%(table_name, i, (i%len(rep_arns)) + 1, (i%len(source_arns))+1, str(datetime.date.today()))
                res['FilterCondition'] = 'between'
                if res['FilterCondition'] == 'ste':
                    res['StartValue'] = row[1]
                elif res['FilterCondition'] == 'gte':
                    res['StartValue'] = row[0]
                    
                else:
                    res['StartValue'] = row[0]
                    res['EndValue'] = row[1]
                if lobMode == 'Y':
                    res['lobMode'] = 'TRUE'
                    res['chunkSize'] = lobSize
                if rename_to:
                    res["renameTable"] = rename_to
                rows.append(res)
                i += 1
            partioned_rows = list(chunks(rows, 30))
            part_count = 1
            for partion in partioned_rows:
                file_name = '%s_%s_PART_%s.%s'%(schema_name, table_name, part_count, 'csv') if len(partioned_rows) > 1 else '%s_%s.%s'%(schema_name, table_name, 'csv')
                with open(os.path.join(path,'output',file_name), 'w') as writeFile:
                    writer = csv.DictWriter(writeFile, fieldnames=fields, lineterminator='\n')
                    writer.writeheader()
                    writer.writerows(partion)
                part_count += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DMS Task options')
    parser.add_argument('--path', required=True, help='Source Path xls')
    parser.add_argument('--lob', required=False, default='N', choices=['Y','N'], help='Lob Mode')
    parser.add_argument('--lobSize', required=False, default='32', help='Lob Size')
    parser.add_argument('--rename', required=False, help='Table Rename To')
    args = parser.parse_args()
    if not args.path:
        print("Invalid Path Provided")
        exit(1)
    process(args.path, args.lob, args.lobSize, rename_to=args.rename)