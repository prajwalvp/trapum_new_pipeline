import os
import sys
import MySQLdb
import numpy as np
import time
import warnings
import optparse
import json

class BaseDBManager(object):
    def __init__(self):
        self.cursor = None
        self.connection = None

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    def with_connection(func):
        """Decorator to make database connections."""
        def wrapped(self,*args,**kwargs):
            if self.connection is None:
                try:
                    self.connection = self.connect()
                    self.cursor = self.connection.cursor()
                except Exception as error:
                    self.cursor = None
                    raise error
            else:
                self.connection.ping(True)
            func(self,*args,**kwargs)
        return wrapped

    @with_connection
    def execute_query(self,query):
        """Execute a mysql query"""
        try:
            self.cursor.execute(query)
        except Exception as error:
            raise error
            #warnings.warn(str(error),Warning)

    @with_connection
    def execute_insert(self,insert):
        """Execute a mysql insert/update/delete"""
        try:
            self.cursor.execute(insert)
            self.connection.commit()
            last_id =  int(self.cursor.lastrowid)
            return last_id
        except Exception as error:
            self.connection.rollback()
            raise error
            #warnings.warn(str(error),Warning)
    @with_connection
    def execute_many(self,insert,values):
        #values is list of tuples
        try:
            self.cursor.executemany(insert,values)
            self.connection.commit()
        except Exception as error:
            self.connection.rollback()
            raise error

    def lock(self,lockname,timeout):
        self.execute_query("SELECT GET_LOCK('%s',%d)"%(lockname,timeout))
        response = self.get_output()
        return bool(response[0][0])

    def release(self,lockname):
        self.execute_query("SELECT RELEASE_LOCK('%s')"%(lockname))

    def execute_delete(self,delete):
        self.execute_insert(delete)

    def close(self):
        if self.connection is not None:
            self.connection.close()
        self.connection = None

    def fix_duplicate_field_names(self,names):
        """Fix duplicate field names by appending
        an integer to repeated names."""
        used = []
        new_names = []
        for name in names:
            if name not in used:
                new_names.append(name)
            else:
                new_name = "%s_%d"%(name,used.count(name))
                new_names.append(new_name)
            used.append(name)
        return new_names

    def get_output(self):
        """Get sql data in numpy recarray form."""
        if self.cursor.description is None:
            return None
        names = [i[0] for i in self.cursor.description]
        names = self.fix_duplicate_field_names(names)
        try:
            output  = self.cursor.fetchall()
        except Exception as error:
            warnings.warn(str(error),Warning)
            return None
        if not output or len(output) == 0:
            return None
        else:
            output = np.rec.fromrecords(output,names=names)
            return output


class TrapumDataBase(BaseDBManager):

    def __init__(self,host,port,name,user,passwd):
        super(TrapumDataBase,self).__init__()
        self.__HOST = host
        self.__NAME = name
        self.__USER = user
        self.__PASSWD = passwd
        self.__PORT = port

    def connect(self):
        return MySQLdb.connect(
            host=self.__HOST,
            db=self.__NAME,
            user=self.__USER,
            passwd=self.__PASSWD,
            port=self.__PORT)


    def create_project(self,name,notes):
        """
        @brief   Creates a new project entry in Projects table     

        @params  Name    Name of the project             
        @params  notes   any extra information for the project 
        """ 
        cols=["name","notes"]
        values = ["'%s'"%name,"'%s'"%notes]
        last_id = self.simple_insert("Projects",cols,values)
        return last_id 

    def create_beamformer_config(self,centre_freq,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp):
        """
        @brief   Creates a new beamformer configuration entry in the Beamformer Configurations table  

        @params  centre_freq           Observation frequency in MHz 
        @params  bandwidth             Bandwidth used in MHz
        @params  incoherent_nchans     Number of channels
        @params  coherent_nchans       Number of channels
        @params  incoherent_tsamp      Sampling time in us
        @params  coherent_tsamp        Sampling time in us
        @params  receiver              Receiver used - NULLABLE
        @params  metadata              Info on other parameters as a key,value pair - NULLABLE

        @return  last_id        last inserted primary key value
        """ 
        cols=["`centre_frequency`","`bandwidth`","`incoherent_nchans`","`coherent_nchans`","`incoherent_tsamp`","`coherent_tsamp`"]
        values = ["'%f'"%centre_freq,"'%f'"%bandwidth,"'%d'"%incoherent_nchans,"'%d'"%coherent_nchans,"'%f'"%incoherent_tsamp,"'%f'"%coherent_tsamp]
        last_id = self.simple_insert("Beamformer_Configuration",cols,values)
        return last_id


    #def create_target(self,project_id,source_name,ra,dec,region,semi_major_axis,semi_minor_axis,position_angle,metadata,notes):
    def create_target(self,project_id,source_name,ra,dec,notes):
        """
        @brief   Creates a target entry in Targets table

        @params  project_id      Project name identifier
        @params  source_name     Name of source to be observed
        @params  ra              Right Ascension in HH::MM::SS
        @params  dec             Declination in DD::MM::SS
        @params  notes           Any extra info about the target    
        Following parameters currently not used as input -> defaulted to NULL
        @params  region          Name of specific sky region e.g. globular cluster
        @params  semi_major_axis Length of semi major axis of elliptic target region (in arcseconds) ??
        @params  semi_minor_axis Length of semi minor axis of elliptic target region (in arcseconds) ??
        @params  position_angle  Angle of source w.r.t plane of sky (in degrees)
        @params  metadata        Info on other parameters as key,value pair       

        @return  last_id        last inserted primary key value
        """ 
        #cols = ["`project_id`","`source_name`","`ra`","`dec`","`region`","`semi_major_axis`","`semi_minor_axis`","`position_angle`","`metadata`","`notes`"]
        #values = ["%d"%project_id,"%s"%source_name,"%s"%ra,"%s"%dec,"%s"%region,"%s"%semi_major_axis,"%s"%semi_minor_axis,
        #          "%s"%position_angle,"%s"%metadata,"%s"%notes]
        cols = ["`project_id`","`source_name`","`ra`","`dec`","`notes`"]
        values = ["%d"%project_id,"'%s'"%source_name,"'%s'"%ra,"'%s'"%dec,"'%s'"%notes]
        last_id = self.simple_insert("Targets",cols,values)
        return last_id

    def create_pointing(self,target_id,bf_config_id,tobs,utc_start,sb_id,mkat_pid,beamshape):
        """
        @brief   Creates a pointing entry in Pointings table     

        @params  target_id    Unique target identifier
        @params  bf_config_id Unique configuration identifier
        @params  tobs         Length of observation (in seconds) 
        @params  utc_start    UTC start time of observation HH:MM:SS
        @params  sb_id        Unique schedule block identifier
        @params  mkat_pid     Internal MeerKAT project identifier
        @params  beamshape    Ellipse parameters of beam as a string

        @return  last_id        last inserted primary key value
        """ 
         
        cols = ["`target_id`","`bf_config_id`","`tobs`","`utc_start`","`sb_id`","`mkat_pid`","`beam_shape`"]     
        vals = ["%d"%target_id,"%d"%bf_config_id,"%f"%tobs,"'%s'"%utc_start,"'%s'"%sb_id,"'%s'"%mkat_pid,"'%s'"%beamshape]
        last_id = self.simple_insert("Pointings",cols,vals)
        return last_id
         

    
    def create_beam(self,pointing_id,on_target,ra,decl,coherent,beam_name):
        """
        @brief   Creates a Beam entry in Beams table     

        @params  pointing_id   Unique pointing identifer for beam             
        @params  on_target     indicates if beam is on or off target: 1 for on 0 for off
        @params  ra            Right Ascension in HH::MM::SS of beam
        @params  decl           Declination in DD::MM::SS of beam
        @params  coherent      indicates if beam is coherent or incoherent: 1 for coherent on 0 for incoherent

        @return  last_id        last inserted primary key value
        """ 
        cols = ["`pointing_id`","`on_target`","`ra`","`decl`","`coherent`","`beam_name`"]
        vals = ["%d"%pointing_id,"%d"%on_target,"'%s'"%ra,"'%s'"%decl,"%d"%coherent,"'%s'"%beam_name]
        last_id = self.simple_insert("Beams",cols,vals)
        return last_id


    def update_tobs(self,pointing_id,tobs):
        """
        @brief   Update length of observation in Pointings table

        @params  pointing_id    Unique pointing identifer 
        @params  tobs           value of observation time

        @return  last_id        last inserted primary key value
        """ 
        cols = "tobs"
        values = tobs
        condition = "pointing_id = %d"%pointing_id # update tobs for particular pointing id 
        last_id = self.simple_update("Pointings",cols,values,condition)
        return last_id


    def create_raw_dataproduct(self,pointing_id,beam_id,state_id,filename,filepath,filehash,file_type_id):
        """
        @brief   Create raw data product entry in Data_Products table, processing_id is NULL by default

        @params  pointing_id    Unique pointing identifer 
        @params  beam_id        Unique Beam identifier
        @params  state_id       If file exists or not: 1 if exists, 2 if deleted, 
        @params  filename       Base filename
        @params  filepath       Path to file
        @params  file_hash      Unique file hash
        @params  file_type_id   Identifier for type of file 
        @params  metadata       Info on other parameters as key value pair
        @params  notes          Any extra info about the target
        @params  upload_date    timestamp of upload in db

        @return  last_id        last inserted primary key value
        """ 
        cols=["pointing_id","beam_id","state_id","filename","filepath","filehash","file_type_id"]
        values=["%d"%pointing_id,"%d"%beam_id,"%d"%state_id,"'%s'"%filename,"'%s'"%filepath,"'%s'"%filehash,"%d"%file_type_id]
        last_id = self.simple_insert("Data_Products",cols,values)
        return last_id
        
        
    def create_secondary_dataproduct(self,pointing_id,beam_id,processing_id,state_id,filename,filepath,file_type_id):
        """
        @brief   Create data product entry in Data_Products table

        @params  pointing_id    Unique pointing identifer 
        @params  beam_id        Unique Beam identifier
        @params  processing_id  Unique processing identifier,NULL if recorded from observation
        @params  file_state_id    If file exists or not: 1 if exists, 0 if deleted,  
        @params  filepath       Path to file
        @params  file_type      Type of file produced 
        @params  metadata       Info on other parameters as key value pair
        @params  notes          Any extra info about the target
        @params  upload_date    timestamp of upload in db

        @return  last_id        last inserted primary key value
        """ 
        cols=["pointing_id","beam_id","processing_id","state_id","filename","filepath","file_type_id"]
        values=["%d"%pointing_id,"%d"%beam_id,"%d"%processing_id,"%d"%state_id,"'%s'"%filename,"'%s'"%filepath,"%d"%file_type_id]
        last_id = self.simple_insert("Data_Products",cols,values)
        return last_id

    def create_full_processing(self,pipeline_id,hardware_id,submit_time,start_time,end_time,process_status,metadata,notes):
        """
        @brief   Create a processing entry in Processings table
        
        @params  pipeline_id    Unique pipeline identifer 
        @params  hardware_id    Unique hardware identifer 
        @params  submit_time    time of submitting job to queue (YYYY-MM-DD HH:MM:SS)
        @params  start_time     Start time of processing (YYYY-MM-DD HH:MM:SS)
        @params  end_time       End time of processing (YYYY-MM-DD HH:MM:SS)
        @params  process_status Status of the process. Options are : 0->submitted, 1->processing, 2->completed
        @params  metadata     Info on other parameters as key value pair
        @params  notes        Any extra info about the target

        @return  last_id        last inserted primary key value
        """ 
        cols = ["pipeline_id","hardware_id","submit_time","start_time","end_time"
                ,"process_status","metadata","notes"]
        vals=["%d"%pipeline_id,"%d"%hardware_id,"'%s'"%submit_time,"'%s'"%start_time,"'%s'"%end_time,
              "'%s'"%process_status,"'%s'"%metadata,"'%s'"%notes]
        last_id = self.simple_insert("Processings",cols,vals)
        return last_id

    def create_processing(self,pipeline_id,submit_time,process_status):
        """
        @brief   Create a processing entry in Processings table for a data product
        
        @params  pipeline_id    Unique pipeline identifer 
        @params  submit_time    time of submitting job to queue (YYYY-MM-DD HH:MM:SS)
        @params  process_status Status of the process. Options are : 0->submitted, 1->processing, 2->completed

        @return  last_id        last inserted primary key value
        """ 
        cols = ["pipeline_id","submit_time","process_status"]
        vals=["%d"%pipeline_id,"'%s'"%submit_time,"'%s'"%process_status]
        last_id = self.simple_insert("Processings",cols,vals)
        return last_id


    def update_submit_time(self,submit_time,processing_id):
        """
        @brief Update the time of job submission to queue

        @params value of submission time (YYYY-MM-DD HH:MM:SS)  
        """
        cols = ["submit_time"]
        values=["%s"%submit_time]
        condition = "processing_id=%d"%processing_id
        self.simple_update("Processings",cols,values,condition)
       

    def update_process_status(self,process_status,processing_id):
        """
        @brief Update the status of a particular processing.Options are : 0->submitted, 1->processing, 2->completed

        @params Value indicating status
        """
        cols = ["process_status"]
        values=["%s"%process_status]
        condition = "processing_id=%d"%processing_id
        self.simple_update("Processings",cols,values,condition)


  
   # When pipeline starts
    def create_pipeline(self,name):
        """
        @brief   Creates a pipeline entry in Pipelines table     

        @params  HASH    unique hash of pipeline                 
        @params  name    unique name of pipeline e.g. presto,peasoup                 
        @params  notes   any extra information for the project 

        @return  last_id        last inserted primary key value
        """ 
        last_id = self.execute_insert("INSERT into Pipelines(`name`) values('%s')"%name)
        return last_id
    
    def create_pivot(self,dp_id,processing_id):
        """
        @brief   Creates a pivot entry in Processing_Pivot table     

        @params  dp_id          Unique dataproducts identifier 
        @params  processing_id  unique Processings identifier                

        @return  last_id        last inserted primary key value
        """ 
        cols = ["dp_id","processing_id",]
        values = ["%s"%dp_id,"%s"%processing_id]
        last_id = self.simple_insert("Processing_Pivot",cols,values)
        return last_id
        

    def update_start_time(self,start_time,processing_id):
        self.execute_query("update Processings set start_time='%s' where processing_id=%d"%(start_time,processing_id)) 
        #print("update Processings set start_time='%s' where processing_id=%d"%(start_time,processing_id)) 


    def update_processing_notes(self,notes,processing_id):
        self.execute_query("update Processings set notes='%s' where processing_id=%d"%(notes,processing_id)) 
       
    def update_processing_status(self,process_status,processing_id):
        self.execute_query("update Processings set process_status='%s' where processing_id=%d"%(process_status,processing_id)) 
        
    def get_pb_from_dp(self,dp_id):
        print("select pointing_id,beam_id from Data_Products where dp_id=%d"%dp_id)
        self.execute_query("select pointing_id,beam_id from Data_Products where dp_id=%d"%dp_id)
        output =  self.cursor.fetchall()
        vals=[]
        return list(list(output)[0])
        #return vals

   
          

 
    def create_hardware_config(self,name,metadata,notes):
        """
        @brief   Creates a hardware configuration in Hardwares table  
    
        @params  name         Name of hardware device 
        @params  metadata     Info on other parameters as key value pair
        @params  notes        Any extra info about the target

        @return  last_id        last inserted primary key value
        """
        cols = ["name","metadata","notes"]
        values = ["'%s'"%name,"'%s'"%metadata,"'%s'"%notes]
        last_id = self.simple_insert("Hardwares",cols,values)
        return last_id

    
    # End of pipeline run 
    
   
    def update_end_time(self,end_time,processing_id):
        self.execute_query("update Processings set end_time='%s' where processing_id=%d"%(end_time,processing_id)) 
    

#    def get_obs_id_from_utc(self,utc):
#        obs = qb.Table("Observations")
#        select = obs.select(obs.obs_id)
#        select.where = (obs.utc_start == utc)
#        query,args = tuple(select)
#        return query%args
   

    def get_project_id(self,description):
        condition = "name LIKE '%s'" % description
        return self.get_single_value("Projects","project_id",condition)
 
    def get_beams_for_pointing(self,pointing_id):
        condition = "pointing_id='%d'" %pointing_id
        return self.get_single_value("Beams","beam_id",condition)



  
    def update_file_status(self,file_status,dp_id):
        cols = ["file_status"]
        values = ["%d" %file_status]
        condition = "dp_id=%d"%dp_id
        self.simple_update("Data_Products",cols,values,condition)

 
    def simple_insert(self,table,cols,values):
        """
        @brief   Inserts new entry into respective table   
    
        @params  table        Table for new entry
        @params  cols         Columns in table to be changed
        @params  values       values for the respective columns to be added
  
        @return  lastrowid    last inserted primary key id
        """
        columns = str(tuple(cols)).replace('\'','')
        vals = str(tuple(values)).replace('\"','')
        print(columns)
        print(vals)
        print("INSERT INTO %s%s VALUES %s"%(table,columns,vals) )
        self.execute_insert("INSERT INTO %s%s VALUES %s"%(table,columns,vals)) 
        return self.cursor.lastrowid

    def simple_update(self,table,cols,values,condition):
        """
        @brief   Updates existing entry in respective table   
    
        @params  table        Table to update
        @params  cols         Columns in table to be updated
        @params  values       values for the respective columns to be updated
        """
        #self.execute_insert("UPDATE '%s' set '%s'='%s' WHERE '%s'"%(table,cols,values,condition))
        print("UPDATE %s set %s=%s WHERE %s"%(table,cols,values,condition))
        
    def get_values(self,table,col,condition):
        """
        @brief   Updates existing entry in respective table   
    
        @params  table        Table to analyse
        @params  cols         Columns in table to be checked
        @params  values       values for the respective columns to be retrieved

	@return  list of values requested
        """ 
        self.execute_query("select %s from %s where %s"%(col,table,condition))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals

################## Delete function ####################

    def delete_table(self,table_name):
        self.execute_delete("delete from %s"%table_name)        

#################### reset auto increment###############
    def reset_auto(self,table_name):
        self.execute_query("alter table %s AUTO_INCREMENT=1"%table_name)






################ Useful Functions ########################

    def get_file_info_from_dp_id(self,dp_id):
        self.execute_query("select filepath,filename from Data_Products where dp_id=%d"%dp_id)
        output = self.cursor.fetchall()
        return output[0][0],output[0][1]


    def get_all_beam_names_for_pointing_id(self,pointing_id):
        condition = "pointing_id=%d"%pointing_id
        return self.get_values("Beams","beam_name",condition)

    def get_project_id_from_name(self,description):
        """
        @brief   Retrieve name of project

        @params  description   Name of project (need not be exact)   
        
        @return  project identifier number  
        """
        condition = "name LIKE '%s'" % description
        return self.get_values("Projects","project_id",condition)

    def get_target_id_from_source_name(self,source_name,project_id):
        """
        @brief Retrieve target_identifier from source name and project identifier

        @params description Name of source and project identifier

        @return target identifier number
        """
        condition = "source_name LIKE '%s' AND project_id LIKE '%d'"%(source_name,project_id)
        return self.get_values("Targets","target_id",condition)

    def get_ra_from_target_id(self,target_id):
        """
        @brief Retrieve source right ascension from target identifier

        @params description Target identifier number

        @return right ascension of target
        """
        condition = "target_id LIKE %d"%target_id
        return self.get_values("Targets","ra",condition)
    
    def get_dec_from_target_id(self,target_id):
        """
        @brief Retrieve source right ascension from target identifier

        @params description Target identifier number

        @return right ascension of target
        """
        condition = "target_id LIKE %d"%target_id
        return self.get_values("Targets","`dec`",condition)


    def get_beam_ids_for_pointing(self,pointing_id):
        """
        @brief   Retrieve all beams for particular pointing

        @params  pointing_id   pointing identifier number
        
        @return  list of beam identifier values  
        """
        condition = "pointing_id LIKE '%s'" %pointing_id
        return self.get_values("Beams","beam_id",condition)

    def check_existing_bf_config_entry(self,centre_frequency,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp):
        """
        @brief Check if there is an existing beamformer config entry for input values

        @params centre_frequency   Observing central frequency
        @params bandwidth          Bandwidth(MHz)
        @params incoherent_nchans  number of channels for incoherent beam
        @params coherent_nchans    number of channels for coherent beam
        @params incoherent_tsamp   sampling time for incoherent beam
        @params coherent_tsamp     sampling time for coherent beam

        @return 0 if entry doesn't exist or 1 if entry exists
        """
        self.execute_query("select exists(select * from Beamformer_Configuration where centre_frequency=%.2f and bandwidth=%.2f and incoherent_nchans=%d and coherent_nchans=%d and incoherent_tsamp LIKE %f and coherent_tsamp LIKE %f)"%(centre_frequency,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def get_existing_bf_config_entry(self,centre_frequency,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp):
        """
        @brief Get existing beamformer configuration id

        @params centre_frequency   Observed central frequency
        @params bandwidth          Bandwidth(MHz)
        @params incoherent_nchans  number of channels for incoherent beam
        @params coherent_nchans    number of channels for coherent beam
        @params incoherent_tsamp   sampling time for incoherent beam
        @params coherent_tsamp     sampling time for coherent beam

        @return beamformer configuration identifier
        """
        self.execute_query("select bf_config_id from Beamformer_Configuration where centre_frequency=%f and bandwidth=%f and incoherent_nchans=%d and coherent_nchans=%d and incoherent_tsamp LIKE %f and coherent_tsamp LIKE %f"%(centre_frequency,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp))
        #print("select bf_config_id from Beamformer_Configuration where centre_frequency=%f and bandwidth=%f and incoherent_nchans=%d and coherent_nchans=%d and incoherent_tsamp LIKE %f and coherent_tsamp LIKE %f)"%(centre_frequency,bandwidth,incoherent_nchans,coherent_nchans,incoherent_tsamp,coherent_tsamp))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def check_existing_pointing_entry(self,target_id,bf_config_id,utc_start,sb_id):
        """
        @brief Check if there is an existing pointing entry

        @params target_id    Target identifier
        @params bf_config_id Beamformer configuration identifier
        @params utc_start    start of recording
        @params sb_id for    schedule block identifier

        @return 0 if entry doesn't exist or 1 if entry exists
        """
        self.execute_query("select exists(select * from Pointings where target_id=%d and bf_config_id=%d and utc_start='%s' and sb_id='%s')"%(target_id,bf_config_id,utc_start,sb_id))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
                
    def get_existing_pointing_id(self,target_id,bf_config_id,utc_start,sb_id):
        """
        @brief Get an existing pointing entry

        @params target_id    Target identifier
        @params bf_config_id Beamformer configuration identifier
        @params utc_start    start of recording
        @params sb_id for    schedule block identifier

        @return pointing identifier
        """
        self.execute_query("select pointing_id from Pointings where target_id=%d and bf_config_id=%d and utc_start='%s' and sb_id='%s'"%(target_id,bf_config_id,utc_start,sb_id))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]


    def get_existing_pointing_id_from_utc_start(self,utc_start):
        """
        @brief Get an existing pointing from the utc start
        
        @params utc_start  start of recording

        @return pointing identifier
        """
        self.execute_query("select pointing_id from Pointings where utc_start='%s'"%(utc_start))
        #print("select pointing_id from Pointings where utc_start='%s'"%(utc_start))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def get_existing_beam_id_from_name(self,beam_name):
        """
        @brief Get an existing beam from utc and beam name

        @params beam name

        @return beam identifier  
        """
        self.execute_query("select beam_id from Beams where beam_name='%s'"%(beam_name))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
    
    def get_existing_beam_id_from_name_and_pointing(self,beam_name,pointing_id):
        """
        @brief Get an existing beam from utc and beam name

        @params beam name
        @params pointing_id

        @return beam identifier  
        """
        self.execute_query("select beam_id from Beams where beam_name='%s' and pointing_id=%d"%(beam_name,pointing_id))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
    
    def check_dp_exists_with_filehash(self,filehash):
        """
        @brief Check if dataproduct already exists using the file hash 

        @params filehash  hash of first 10MB of file along with size of file

        @return  1 if DP exists in table ; 0 if not
        """    
        self.execute_query("select exists(select * from Data_Products where filehash='%s')"%(filehash))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
        
    def get_existing_dp_id_from_filehash(self,filehash):
        """
        @brief Get dataproduct that already exists using the file hash 

        @params filehash  hash of first 10MB of file along with size of file

        @return  1 if DP exists in table ; 0 if not
        """    
        self.execute_query("select dp_id from Data_Products where filehash='%s'"%(filehash))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def check_id_from_pipeline_name(self,name):
        """
        @brief Check if pipeline already exists in Pipelines Table

        @params name Name of pipeline

        @return pipeline identifier number
        """

        self.execute_query("select exists(select pipeline_id from Pipelines where name='%s')"%(name))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
    
    def get_existing_pipeline_id_from_name(self,name):
        """
        @brief Get pipeline id which already exists in Pipelines Table

        @params name Name of pipeline

        @return pipeline identifier number
        """

        self.execute_query("select pipeline_id from Pipelines where name='%s'"%(name))
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def get_non_processed_dp_ids_for_pipeline(self,name):
        """
        @brief Get all dataproduct ids which haven't been processed by pipeline

        @params name

        @return list of data product ids to be processed
        """
        self.execute_query() # Need to put complex query. 
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]

    def get_filepath_from_dp_id(self,dp_id):
        """
        @brief Get file path from dataproducts table 

        @params dp_id dataproduct identifier

        @return path to file location
        """
        self.execute_query("select file_path from Data_Products where dp_id = %d"%dp_id) # Need to put complex query. 
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
 
    def get_filetype_from_dp_id(self,dp_id):
        """
        @brief Get file type from dataproducts table 

        @params dp_id dataproduct identifier

        @return type of file
        """
        self.execute_query("select file_type from Data_Products where dp_id = %d"%dp_id) # Need to put complex query. 
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
 
    def get_metadata_from_dp_id(self,dp_id):
        """
        @brief Get file path from dataproducts table 

        @params dp_id dataproduct identifier

        @return metadata of file
        """
        self.execute_query("select metadata from Data_Products where dp_id = %d"%dp_id) # Need to put complex query. 
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]
 

    def get_non_processed_data_products(self):
        """
        @brief   Retrieve data_products which have not been processed at all
        
        @return  list of Data Product identifier values 
        """
        condition = "processing_id IS NULL"
        return self.get_values("Data_Products","dp_id",condition)

    #def get_non_processed_dp_ids_for_pipeline(self,pipeline):
        


    def get_data_products_for_pointing(self,pointing_id):
        """
        @brief   Retrieve data products for particular pointing

        @params  pointing_id          pointing identifier number
        
        @return  list of data product identifier values
        """
        condition = "pointing_id LIKE %s"%pointing_id
        return self.get_values("Data_Products","dp_id",condition)

    def get_pointings_for_target(self,target_id):
        """
        @brief   Retrieve pointings for particular target 

        @params  target_id    target identifier number
        
        @return  list of pointing identifier values
        """
        condition = "target_id LIKE %s"%target_id
        return self.get_values("Pointings","pointing_id",condition)

    def get_data_products_from_processing(self,processing_id):
        """
        @brief   Retrieve data products from particular processing

        @params  processing_id   processing identifier value 
        
        @return  list of data product identifier values 
        """
        condition = "processing_id LIKE %s"%processing_id
        return self.get_values("Processing_Pivot","dp_id",condition)

    def get_dp_id_from_filename(self,filename): ### !!! Maybe add filepath for nested ?
        condition = "filename like '%s'"%filename
        return self.get_values("Data_Products","dp_id",condition)

    def get_pipeline_id_from_name(self,name):
        self.execute_query("select pipeline_id from Pipelines where name like '%s'"%name)
        output =  self.cursor.fetchall()
        vals=[]
        for i in range(len(output)):
            vals.append(list(list(output)[i])[0])
        return vals[0]


    def full_path_of_dp_id(self,dp_id):
        self.execute_query("select select filepath,filename from Data_Products where dp_id=%d"%dp_id)
        output =  self.cursor.fetchall()
        vals=[]
        file_details =  list(list(output)[0])
        return str(file_details[0])+str(file_details[1])
          


    
if __name__ =='__main__':

    # Setup connection
    trapum_db = TrapumDataBase();
    con = trapum_db.connect()
    con_cursor = con.cursor()
