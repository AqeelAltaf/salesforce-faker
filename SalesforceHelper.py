from simple_salesforce import Salesforce
from simple_salesforce import SFType
from functools import lru_cache
import pandas as pd
from .SalesforceDataGenerator import SalesforceDataGenerator

class SalesforceFaker:
    def __init__(self,instance_url,username,password,security_token,sandbox):
        self.data_gen = SalesforceDataGenerator()
        self.instance_url =instance_url
        self.username = username
        self.password = password
        self.sandbox = sandbox
        self.security_token = security_token
        table_ids = pd.read_csv('uat_parent_lists.csv')
        table_ids.fillna('',inplace = True)
        table_ids =  table_ids[(table_ids['id_in_sf_gcp'].str.len() ==18) & (table_ids['id_in_sf_gcp'] != 'Data creation prob') ]
        table_ids.set_index('object',drop = True, inplace = True)
        self.object_id_dict = table_ids.to_dict()['id_in_sf_gcp']
        self.sf = Salesforce(instance_url=self.instance_url,username=self.username,password=self.password,security_token=self.security_token,sandbox=self.sandbox)

    
    def get_pick_list_values(self,field):
        id_list = list(map(lambda x : x['label'],field['picklistValues']))
        if  id_list:
            return id_list
        else:
            return ['None']

    @lru_cache(maxsize=128)
    def get_list_of_ids_for_table(self,ref_table_name):  
        if self.object_id_dict.get(ref_table_name,None) != None:
            return [self.object_id_dict.get(ref_table_name,None)]
        else:
            id_list = list(map(lambda x : x['Id'], self.sf.query(f'SELECT Id FROM {ref_table_name} limit 100 ')['records']))
            if id_list:
                return id_list
            else:
                return None
            

    def generate_data_for_table(self,table_name,num_rows=10 , reference = True, drop_id = True ):
        '''
        This function generates data for the given table name of the saleforce

        table_name(str) : API_NAME of salesforce
        num_rows(int)  : number of rows to generate
        reference(bool) : flag to generate data for reference field or not

          Returns:
          DataFrame : generated data  dataframe

        '''
        dict_for_default = {'CurrencyIsoCode': '',
        'IsDeleted': 'False',
        'Is_Processed__c': 'False'}

        details = getattr(self.sf, table_name).describe()

        # these are columns that salesforce generates itself when someone creates data 
        columns_not_to_add = ["CreatedById",
                            "OwnerId",
                            "LastModifiedById"
                            "LastModifiedDate"
                            ,"Id",
                            "CreatedDate",
                            "SystemModstamp",
                            "LastActivityDate",
                            "LastViewedDate",
                            "LastReferencedDate",
                            "CreatedById",
                            "LastReferencedDate",
                            "LastModifiedById",
                            "LastModifiedDate"]
        # for field in details['fields']:
        #     print(field['name'])
        field_name_type = {field['name'] : field['type'] for field in details['fields'] if field['type'] != 'reference' and field['createable'] == True and  field['name'] not in columns_not_to_add}
        # print(field_name_type)
        field_conf = {field['name'] : {'picklistvalues': self.get_pick_list_values(field)} for field in details['fields'] if field['type']  in ['multipicklist','picklist','combobox']  and field['nillable'] == False }
        if reference == True:
            for field in details['fields']:
                # this is done temporarily as we're using all the references(not just non-nuillable)
                # if field['type'] == 'reference'    and field['name'] not in columns_not_to_add :
                if field['type'] == 'reference'    and field['name'] not in columns_not_to_add  and field['nillable'] == False:
                    field_name_type[field['name']] = 'picklist'
                    field_values = self.get_list_of_ids_for_table(field.get('referenceTo',[None])[0])
                    if field_values is not None:
                        field_conf[field['name']] = {'picklistvalues': field_values }
                    # agar ye nillable nahi hai to 
                    # elif field['nillable'] == True:
                    #     field_conf[field['name']] = {'picklistvalues':[None]}
                    else:
                        raise ValueError(f'There is no value in table {field["name"]} to use it as a foreign key')
        for key, value in dict_for_default.items():
            if key in field_name_type:
                field_name_type[key] = 'picklist'
                field_conf[key]  = {"picklistvalues" : [dict_for_default.get(key, "None")]}
        # return field_name_type , field_conf
        return self.data_gen.generate_dataframe(column_names= list(field_name_type.keys()), column_types = list(field_name_type.values()), num_rows=num_rows, conf = field_conf)


    def generate_csv_for_table(self,table_name, num_rows=10 ,reference = True,filename = 'data.csv'):
        '''
        This function generates data for the given table name of the saleforce and saves it in csv file

        table_name(str) : API_NAME of salesforce
        num_rows(int)  : number of rows to generate
        reference(bool) : flag to generate data for reference field or not
        filename(str) : name of file to save generated data 

          Returns:
          file (csv) : saves generated data in csv 

        '''
        df = self.generate_data_for_table(table_name,num_rows=num_rows ,reference = reference)
        df.to_csv(filename, index = False)



## todo 
# give option to insert data in salesforce