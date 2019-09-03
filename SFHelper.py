from simple_salesforce import Salesforce
from simple_salesforce import SFType
from functools import lru_cache

class SFHelper:
    def __init__(self,instance_url,username,password,security_token,sandbox)
        self.instance_url =instance_url
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.sf = Salesforce(instance_url=self.instance_url,username=self.username,password=self.password,security_token=self.security_token,sandbox=self.sandbox)
    
    def get_pick_list_values(field):
        id_list = list(map(lambda x : x['label'],field['picklistValues']))
        if not id_list:
            return id_list
        else:
            return ['None']

    @lru_cache(maxsize=128)
    def get_list_of_ids_for_table(ref_table_name):
        id_list = list(map(lambda x : x['Id'], sf.query(f'SELECT Id FROM {ref_table_name} limit 100')['records']))
        if id_list:
            return id_list
        else:
            return [None]
    

def generate_data_for_table(table_name, reference = True,sfaker):
    details = getattr(sf, table_name).describe()
    field_name_type = {field['name'] : field['type'] for field in details['fields'] if field['type'] != 'reference'}
    field_conf = {field['name'] : {'picklistvalues': get_pick_list_values(field)} for field in details['fields'] if field['type']  in ['multipicklist','picklist','combobox']}
    if reference == True:
        for field in details['fields']:
            if field['type'] == 'reference':
                field_name_type[field['name']] = 'picklist'
                field_conf[field['name']] = {'picklistvalues': get_list_of_ids_for_table(field.get('referenceTo',[['None']])[0]) }   
    return sfaker.generate_dataframe(column_names= list(field_name_type.keys()), column_types = list(field_name_type.values()), num_rows=10, conf = field_conf)
