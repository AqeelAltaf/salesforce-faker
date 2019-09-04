from simple_salesforce import Salesforce
from simple_salesforce import SFType
from functools import lru_cache

class SFHelper:
    def __init__(self,instance_url,username,password,security_token,sandbox):
        self.instance_url =instance_url
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.security_token = security_token
        self.sf = Salesforce(instance_url=self.instance_url,username=self.username,password=self.password,security_token=self.security_token,sandbox=self.sandbox)
    
    def get_pick_list_values(self,field):
        id_list = list(map(lambda x : x['label'],field['picklistValues']))
        if not id_list:
            return id_list
        else:
            return ['None']

    @lru_cache(maxsize=128)
    def get_list_of_ids_for_table(self,ref_table_name):  
        id_list = list(map(lambda x : x['Id'], self.sf.query(f'SELECT Id FROM {ref_table_name} limit 100')['records']))
        if id_list:
            return id_list
        else:
            return None
        

    def generate_data_for_table(self,table_name, reference = True):
        '''
        '''
        details = getattr(self.sf, table_name).describe()
        field_name_type = {field['name'] : field['type'] for field in details['fields'] if field['type'] != 'reference'}
        field_conf = {field['name'] : {'picklistvalues': self.get_pick_list_values(field)} for field in details['fields'] if field['type']  in ['multipicklist','picklist','combobox']}
        if reference == True:
            for field in details['fields']:
                if field['type'] == 'reference':
                    field_name_type[field['name']] = 'picklist'
                    field_values = self.get_list_of_ids_for_table(field.get('referenceTo',[None])[0])
                    if field_values is not None:
                        field_conf[field['name']] = {'picklistvalues': field_values }
                    elif field['nillable'] == True:
                        field_conf[field['name']] = {'picklistvalues':[None]}
                    else:
                        raise ValueError(f'There is no value in table {field["name"]} to use it as a foreign key')
        return field_name_type , field_conf
        # return sfaker.generate_dataframe(column_names= list(field_name_type.keys()), column_types = list(field_name_type.values()), num_rows=10, conf = field_conf)
