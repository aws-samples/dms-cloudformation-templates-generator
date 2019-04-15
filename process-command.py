import argparse, subprocess, json, csv, os

class AWSCLI:
    def __init__(self):
        pass

    def processCommand(self,command):
        out,err = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True).communicate()
        if err:
            print ("Command Error: ",err)
        else:
            return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create CloudFormation Stacks')
    parser.add_argument('--cmd', required=True)
    parser.add_argument('--output_type', default='json')
    args = parser.parse_args()
    cli = AWSCLI()
    cmd = args.cmd
    if args.output_type == 'text':
        cmd = cmd + "--output=text"
    cli.processCommand(cmd + "> output.log" )
