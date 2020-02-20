import pandas as pd
import random
from faker import Faker
from datetime import datetime
import time

class SalesforceDataGenerator:

    def __init__(self):
        '''Initializing class'''
        self.fake = Faker()

    def gen_date(self):
        ''' This function generates  date in format of MM/dd/yyyy
        
        Returns:
        Date :   format of MM/dd/yyyy
        '''
        return  self.fake.date(pattern='%Y-%m-%d')

    def get_float(self, min_value=0 , max_value=100):
        return round(self.fake.pyfloat(min_value=min_value, max_value=max_value),2)
    

    def get_currency_float(self):
        return round(self.fake.pyfloat(min_value=1, max_value=100),2)

    def gen_from_multipicklist(self,picklist_values):
        '''
        This function generates more than one random comma separated values from given picklist values

        Parameters:
        picklist_values (list): all possible values of picklist

        Returns:
        str: random values(comma separated) from given picklist_values 

        '''
        return ';'.join(random.choices([str(_) for _ in picklist_values], k=random.randint(1,len(picklist_values))))

    def gen_from_picklist(self,picklist_values):
        '''
        This function picks data from the given picklist values

        Parameters:
        picklist_values (list): all possible values of picklist

        Returns:
        str: random value from given picklist_values 

        '''
        return random.choice(picklist_values) 

    def gen_from_combobox(self,picklist_values):
        '''
        This function picks data from the given picklist values , i

        Parameters:
        picklist_values (list): all possible values of picklist

        Returns:
        str: random value from given picklist_values '''
        
        picklist_values.append(self.fake.pystr())
        return random.choice(picklist_values) 

    def  gen_base64(self):
        """This function generates base64 type string
        
        Returns:
        str :  base64 type string

        """
        return self.fake.pystr(150,max_chars=1000)

    def gen_percentage(self):
        """This function returns random percentage string
        
        Returns:
        str : returns random percentage string example 19.4%
        """
        return round(self.fake.pyfloat(min_value=0, max_value=100),2)
        # return f"{random.uniform(0,100):.2f}%"

    def generate_location(self):
        """"""

        self.fake.location_on_land()[:2]


    def get_generator(self,dtype = 'anyType'):
        """ This is a helper function which returns generator for given datatype


        Paramters:
        dtype (str) : data type for which generate is needed

        Returns:
        method : generator for given datatype

        """
        # checking if dtype is str
        if type(dtype) != str:
            raise ValueError("Data type must be of type str, found " + str(type(dtype)))

        dt_mapping =  {'address': self.fake.address,
        'anytype' : self.fake.pystr,
        'boolean':self.fake.boolean,
        'base64' : self.gen_base64,
        'combobox': self.gen_from_combobox,
        #  'complexvalue' : ,
        'currency' : self.get_currency_float,
        'date' : self.gen_date,
        'datetime' : self.gen_date,
        'double': self.get_float ,
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
        """ This method returns generated value for given datatype 

        Parameters:
        dtype (str) : data type for which generate is needed

        Returns:
        multiple: it returns data of given dtype
        

        """
        # if data type is not  'multipicklist','picklist' or 'combobox' then generated data using faker 
        if dtype.lower()  in ['multipicklist','picklist','combobox']:
            return self.get_generator(dtype= dtype)(conf.get('picklistvalues',[None]))
        # else select value from given list in config
        else:
            return self.get_generator(dtype= dtype)()
            


    def gen_data_series(self,dtype = 'anyType', number = 10,conf = {}):
        """
        This function returns DataSeries of given datatype  for the desired length/numbers.  
        (DataSeries is like a list, or column in database)

        Parameters:
        dtype (str) : data type for which generate is needed
        number(int) : number of elements wanted in the DataSeries 
        conf(dict)  : it is if any to pass extra information about the field.
                     We need this while generating data for   'multipicklist','picklist' or 'combobox'  
        Returns:
        DataSeries  : Data Series for the generated data
        """
        try:
            length = int(number)
        except:
            raise ValueError("Number of samples must be a positive integer, found " + number)

        if number <= 0:
            raise ValueError("Number of samples must be a positive integer, found " + number)
        return pd.Series(self.generate_random(dtype,conf) for _ in range(0,length))


    def generate_dataframe(self,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):

        '''
        This function generates dataframe for the given parameters
       

        Parameters:
        column_names(list) : list of column names
        column_types(list) : list of column data types
        num_rows(int)  : number of rows to generate
        conf(dict)  : it is if any to pass extra information about the field.
                       We need this while generating data for   'multipicklist','picklist' or 'combobox'  

        Returns : 
        DataFrame : generated data  dataframe
        '''
        if len(column_names) != len(column_types):
            raise ValueError(f"Length of column_names and column_types mismatched lenght of names = {len(column_names)} Length of column_types = {len(column_types)}")
        return pd.DataFrame({column_name : self.gen_data_series(dtype = column_type,number = num_rows,conf =  conf.get(column_name,{})) for column_name, column_type in zip(column_names, column_types)})

    def generate_csv(self,filename = 'data.csv' ,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):

        '''
         This function generates dataframe for the given parameters and saves it in csv
       

        Parameters:
        filename(str) : name of file to save generated data 
        column_names(list) : list of column names
        column_types(list) : list of column data types
        num_rows(int)  : number of rows to generate
        conf(dict)  : it is if any to pass extra information about the field.
                       We need this while generating data for   'multipicklist','picklist' or 'combobox'  

        Returns : 
        DataFrame : generated data  dataframe/saves csv file
        '''
        
        df = self.generate_dataframe(column_names , column_types ,num_rows,conf)
        df.to_excel(filename)
        return df

    def generate_excel(self,filename = 'data.xlsx' ,column_names = ['Id'], column_types = ['anytype'], num_rows = 10, conf = {}):
        '''
        This function generates dataframe for the given parameters and saves it in excel
       

        Parameters:
        filename(str) : name of file to save generated data 
        column_names(list) : list of column names
        column_types(list) : list of column data types
        num_rows(int)  : number of rows to generate
        conf(dict)  : it is if any to pass extra information about the field.
                       We need this while generating data for   'multipicklist','picklist' or 'combobox'  

        Returns : 
        DataFrame : generated data  dataframe/saves excel file
        '''
        
        df = self.generate_dataframe(column_names , column_types ,num_rows,conf )
        df.to_csv(filename)
        return df

# sf = SalesforceFaker()
# sf.generate_excel('file.xlsx',['address1','time','date_','value'],['address','time','date','picklist'],50,conf = {'value':{'picklistvalues':['one','two']}})

