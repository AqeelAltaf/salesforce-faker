import pandas as pd
import random
from faker import Faker
from .SalesforceDataGenerator import SalesforceDataGenerator

class SalesforceDataGenerator:

    def __init__(self):
        self.fake = Faker()


    def gen_from_multipicklist(self,picklist_values):

        return ','.join(random.choices([str(_) for _ in picklist_values], k=random.randint(1,len(picklist_values))))

    def gen_from_picklist(self,picklist_values):
        return random.choice(picklist_values) 

    def gen_from_combobox(self,picklist_values):
        picklist_values.append(self.fake.pystr())
        return random.choice(picklist_values) 

    def  gen_base64(self):
        return self.fake.pystr(150,max_chars=1000)

    def gen_percentage(self):
        """This function returns random percentage"""
        return f"{random.uniform(0,100):.2f}%"

    def generate_location(self):
        self.fake.location_on_land()[:2]


    def get_generator(self,dtype = 'anyType'):
        
        if type(dtype) != str:
            raise ValueError("Data type must be of type str, found " + str(type(dtype)))

        dt_mapping =  {'address': self.fake.address,
        'anytype' : self.fake.pystr,
        'boolean':self.fake.boolean,
        'base64' : self.gen_base64,
        'combobox': self.gen_from_combobox,
        #  'complexvalue' : ,
        'currency' : self.fake.currency_code,
        'date' : self.fake.date,
        'datetime' : self.fake.date_time,
        'double': self.fake.pyfloat ,
        'email' : self.fake.email,
        'id' :self.fake.pystr,
        'int' : self.fake.pyint,
        'location': self.generate_location,
        'multipicklist': self.gen_from_multipicklist,
        'percent' : self.gen_percentage,
        'phone': self.fake.phone_number,
        'picklist':  self.gen_from_picklist,
        #  'reference': ,
        'string': self.fake.word,
        'textarea': self.fake.text,
        'time' : self.fake.time,
        'url' : self.fake.url   }
        if dtype.lower() not in dt_mapping.keys():
            raise ValueError(f"{dtype} is not supported in salesforce, select from {list(dt_mapping.keys())}")
        
        return dt_mapping[dtype.lower()]




    def generate_random(self,dtype='anyType',conf = {}):
        
        if dtype.lower() not in ['multipicklist','picklist','combobox']:
            return self.get_generator(dtype= dtype)()
        else:
            return self.get_generator(dtype= dtype)(conf.get('picklistvalues',[None]))

    def gen_data_series(self,dtype = 'anyType', number = 10,conf = {}):
        """
        This function returns DataSeries of given datatype  for the desired length/numbers.
        
        """
        try:
            length = int(number)
        except:
            raise ValueError("Number of samples must be a positive integer, found " + number)

        if number <= 0:
            raise ValueError("Number of samples must be a positive integer, found " + number)
        return pd.Series(self.generate_random(dtype,conf) for _ in range(0,length))


    def generate_dataframe(self,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):
        if len(column_names) != len(column_types):
            raise ValueError(f"Length of column_names and column_types mismatched lenght of names = {len(column_names)} Length of column_types = {len(column_types)}")
        return pd.DataFrame({column_name : self.gen_data_series(dtype = column_type,number = num_rows,conf =  conf.get(column_name,{})) for column_name, column_type in zip(column_names, column_types)})

    def generate_csv(self,filename = 'data.csv' ,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):
        df = self.generate_dataframe(column_names , column_types ,num_rows,conf )
        df.to_excel(filename)
        return df

    def generate_excel(self,filename = 'data.xlsx' ,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):
        df = self.generate_dataframe(column_names , column_types ,num_rows,conf )
        df.to_csv(filename)
        return df

# sf = SalesforceFaker()
# sf.generate_excel('file.xlsx',['address1','time','date_','value'],['address','time','date','picklist'],50,conf = {'value':{'picklistvalues':['one','two']}})
## TODO 
# adding comments
