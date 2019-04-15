import argparse, subprocess, json, csv, os


header = ['taskName','taskARN']


class AWSCLI:
    def __init__(self):
        pass

    def processCommand(self,command):
        print (command)
        out,err = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True).communicate()
        #print (out)
        if err:
            print ("Command Error: ",err)
        else:
            output = json.loads(out.strip())
            return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create CloudFormation Stacks')
    parser.add_argument('--stackName', required=True)
    args = parser.parse_args()
    cli = AWSCLI()
    output = cli.processCommand("aws cloudformation describe-stack-resources --stack-name=%s"% args.stackName)
    if not os.path.exists("task-arns"): # Creating output folder if not exists
        os.makedirs("task-arns")
    with open(os.path.join('task-arns', args.stackName + '.csv'), 'w') as writefile:
        writer = csv.DictWriter(writefile, fieldnames=header)
        writer.writeheader()
        taskArns = [{'taskName':res.get("LogicalResourceId"),'taskARN':res.get("PhysicalResourceId")} for res in output["StackResources"]]
        writer.writerows(taskArns)
