import random
import time
import string
import json

# metric_name_list=['mem.total_mb', 'mem.usable_mb', "vm.cpu.utilization_perc","cpu.idle_perc","cpu.total_logical_cores",
#                   "disk.total_used_space_mb","nova.vm.disk.total_allocated_gb", "swiftlm.diskusage.host.val.size"]
host_name_list = ['pg-tips%s'%i for i in xrange(10)]
zone_list = ['nova']
service_list = ['compute','system']
control_plane_list = ['ccp']
component_list = ['vm']
cluster_list = ['compute']
cloud_name_list = ['monasca']
tenant_id_list = [ "90273cc79acc4239816d572f9397863e"]



class Metric(object):
    def __init__(self, metric_name):
        self._metric_name = unicode_to_ascii(metric_name)
        self._schema = self.get_schema()

    def get_schema(self):
        with open('./metric_templates.json', 'r') as f:
            json_obj = json.load(f)
            metric_dict = json_obj[self._metric_name]
            return metric_dict

    def construct_output_dict(self, metric_dict):
        output_dict = {}
        for k,v in metric_dict.items():
            k =  unicode_to_ascii(k)
            if isinstance(v, dict):
                output_dict[k] = self.construct_output_dict(v)
            else:
                if isinstance(v, (str,int, float, long)) or '(' not in v:
                    if isinstance(v, basestring):
                        v = unicode_to_ascii(v)
                    output_dict[k] = v
                else:
                    value = None
                    try:
                        value = eval(v)
                    except Exception as ex:
                        pass

                    output_dict[k] = value

        return output_dict

    def get_metric_dict(self):

        return self.construct_output_dict(self.get_schema())

def unicode_to_ascii(u):
    return u.encode('ascii', errors='backslashreplace')

def get_timestamp():

    #timestamp in miniseconds
    time_stamp = ((int)(time.time() - 120)) * 1000
    return time_stamp

def get_creation_time():
    return (int)(time.time() -120)

def get_metric_template(metric_name):
    with open('./metric_templates.json', 'r') as f:
        # str_from_file = f.read()
        json_obj = json.load(f)
        metric_dict = json_obj[metric_name]
        metric_template = string.Template(json.dumps(metric_dict))
        return metric_template



def generate_metrics():
    '''

    :return: a non-stop generator that yield a dictionary object contains metric information
    '''
    metric_name_list = []
    with open('./metric_templates.json', 'r') as f:
        json_obj = json.load(f)
        metric_name_list = json_obj.keys()
    metric_name_list = map(unicode_to_ascii, metric_name_list)
    while True:
        metric_name = random.choice(metric_name_list)
        metric_dict = Metric(metric_name).get_metric_dict()
        # yield get_metric_template('mem.total_mb').substitute(metric_dict)
        yield metric_dict

def get_random_resource_id():
    resource_id_str = get_random_hex_string(36)
    resource_id_list = list(resource_id_str)

    # to replace these locations with '-', so it complies with the form of
    # resource_id : 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    for i in [8,13,18,23]:
        resource_id_list[i] = '-'
    return ''.join(resource_id_list)

def get_random_hex_string(char_num_of_str=1):
    char_list = [random.choice(string.hexdigits) for _ in xrange(char_num_of_str)]
    return ''.join(char_list)


resource_id_list = [ get_random_resource_id() for _ in xrange(10)]


if __name__ == '__main__':

    for _ in xrange(30):
        print next(generate_metrics())