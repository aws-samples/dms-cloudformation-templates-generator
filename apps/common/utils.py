import csv

def read_csv(file_name):
    """
    Read the csv file and return the list of dict from the data.
    """

    json_dict = list()
    with open(file_name) as csvdata:
        json_dict = list(csv.DictReader(csvdata))
    return json_dict


def add_outputs(resources):
    """
    Return outputs for resources
    """

    output = { r : {'Value':{'Ref': r}} for r in resources}
    return output