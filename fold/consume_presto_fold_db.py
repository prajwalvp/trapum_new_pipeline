import glob
import xml.etree.ElementTree as ET
import subprocess
import optparse
from optparse import OptionParser
import numpy as np
import sys
import time
import logging
import re
import pika_process
import json
import trapum_db_fold
from datetime import datetime


log = logging.getLogger('manual_presto_fold')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)


def period_modified(p0,pdot,no_of_samples,tsamp,fft_size):
    if (fft_size==0.0):
        return p0 - pdot*float(1<<(no_of_samples.bit_length()-1))*tsamp/2
    else:
        return p0 - pdot*float(fft_size)*tsamp/2

def a_to_pdot(P_s, acc_ms2):
    LIGHT_SPEED = 2.99792458e8                 # Speed of Light in SI
    return P_s * acc_ms2 /LIGHT_SPEED


def middle_epoch(epoch_start, no_of_samples, tsamp):
     return epoch_start +0.5*no_of_samples*tsamp 

def extract_and_fold(opts,info):

    xml={}

    xml_file = info["xml_path"]+'/overview.xml'
    tree = ET.parse(xml_file)
    root = tree.getroot()

    #initiate empty arrays
    mod_period=[]
    period=[]
    acc=[]
    pdot=[]
    dm=[]
    snr=[]

    #datetime parameters
    xml['datetime'] = root.find('misc_info/utc_datetime').text.replace(":","-")
    #Header Parameters
    xml['ra'] = root.find('header_parameters/src_raj').text
    xml['dec'] = root.find('header_parameters/src_dej').text
    source_name = root.find('header_parameters/source_name').text
    xml['source_name'] = source_name.replace(" ","").replace(":","").replace(",","")
    #raw_data_filename=root.find('header_parameters/rawdatafile').text 
    xml['epoch_start'] = float(root.find("header_parameters/tstart").text)
    xml['tsamp'] = float(root.find("header_parameters/tsamp").text)
    xml['no_of_samples'] = int(root.find("header_parameters/nsamples").text)

    #Search Parameters
    xml['infile_name'] = root.find("search_parameters/infilename").text
    xml['killfile_name'] = root.find("search_parameters/killfilename").text
    xml['fft_size'] = float(root.find('search_parameters/size').text)


    for P in root.findall("candidates/candidate/period"):
        period.append(float(P.text))
    for A in root.findall("candidates/candidate/acc"):
        acc.append(float(A.text))
    for D in root.findall("candidates/candidate/dm"):
        dm.append(float(D.text))
    for s in root.findall("candidates/candidate/snr"):
        snr.append(float(s.text))
    
    for i in range(len(period)):
        Pdot = a_to_pdot(period[i],acc[i])
        mod_period.append(period_modified(period[i],Pdot,xml['no_of_samples'],xml['tsamp'],xml['fft_size']))
        pdot.append(Pdot)


    # Get number of candidates
    tmp1 = subprocess.getoutput("grep \'candidate id\' %s | tail -1 | awk \'{print $2}\'| grep -o \'.*\'"%xml_file)
    no_of_cands  = int(re.findall(r"'(.*?)'", tmp1, re.DOTALL)[0])+1
         

    # Set all paths
    input_file = xml['infile_name']
    output_path = info["output_path"]
 
    #input_name = input_file
    source_name = xml['source_name']    
    #mask_path = xml['killfile_name'].split(".")[0]+".mask"
    mask_path = "/beegfs/u/prajwalvp/trapum_processing/mask_for_beam2_Ter5_16apr20/Ter5_full_res_stats_time_2_rfifind.mask" #Hardcoded

    batch_no = info["batch_number"]


    # Make the output directory
    try:
        subprocess.check_call("mkdir -p %s"%info['output_path'],shell=True)
    except:
        log.info("Subdirectory already made")
        pass

   
    # Setup database connection
    log.debug("Connecting to TrapumDB...")
    try:
        trapum = trapum_db_fold.TrapumDataBase("host ip","port","db_name","user","passwd");
        t =trapum.connect()
    except:
        log.error("DB connection failed")
        pass
    log.debug("Connected to TrapumDB")

    # Get group of filenames
    input_name = info['subgroup_files']

    # Get processing id
    processing_id = info['processing_id']

    # Create_processing_pivot_entries
    dp_ids = info["dp_ids"]
    pp_ids=[]
    for dp_id in dp_ids:
        pp_id = trapum.create_pivot(dp_id,processing_id)
        pp_ids.append(pp_id)

    # Start time update
    start_time = str(datetime.now())
    trapum.update_start_time(start_time,processing_id)

    # No of cands manual for testing
    #no_of_cands=1

    # Run in batches
    extra = no_of_cands%batch_no
    batches = int(no_of_cands/batch_no) +1
    for x in range(batches):
       start = x*batch_no
       if(x==batches-1):
           end = x*batch_no+extra
       else:
           end = (x+1)*batch_no   
       for i in range(start,end):
           folding_packet={}
           folding_packet['period'] = mod_period[i]
           folding_packet['acc'] = acc[i]
           folding_packet['pdot'] = pdot[i] 
           folding_packet['dm'] = dm[i] 
           output_name= "dm_%.2f_acc_%.2f_candidate_number_%d"%(folding_packet['dm'],folding_packet['acc'],i)
           try:
               process = subprocess.Popen("prepfold -ncpus 1 -mask %s -noxwin -nodmsearch -topo -p %s -pd %s -dm %s %s -o %s"%(mask_path,str(folding_packet['period']),str(folding_packet['pdot']),str(folding_packet['dm']),input_name,output_name),shell=True,cwd=output_path)
           except Exception as error:
               trapum.update_processing_status("Failed",processing_id)
               trapum.update_processing_notes(str(error).replace("'",""),processing_id)
               log.error(error)
 
       if  process.communicate()[0]==None:
           continue
       else:
           time.sleep(10)


    # End time update
    end_time = str(datetime.now())
    trapum.update_end_time(end_time,processing_id)
    trapum.update_processing_status("Successful",processing_id)


    # Get pointing and beam id from one of the dp_ids
    try:
        pb_list = trapum.get_pb_from_dp(dp_id)
    except Exception as error:
        log.error(error)

    # Update Dataproducts with peasoup output entry
    dp_id = trapum.create_secondary_dataproduct(pb_list[0],pb_list[1],processing_id,1,"unscored_cands",str(output_path),10)

    # Create scoring processing
    



    # Create PICS scoring packet
    pics_info={}
    pics_info["dp_id"] = dp_id
    pics_info["processing_id"] = processing_id
    pics_info["input_path"] = info["output_path"]
    pics_info["model"] = "clfl2_PALFA.pkl"  # Best known model
    
    
    # Publish to Rabbit
    log.debug("Ready to publish to RabbitMQ")
    opts.queue = "pics_original_Ter5_all_beams"
    pika_process.publish_info(opts,pics_info)



def receive_message(message,opts):
    global info
    info = json.loads(message.decode("utf=8"))
    extract_and_fold(opts,info)

                
if __name__=='__main__':

    # Update all input arguments
    consume_parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(consume_parser)
    opts,args = consume_parser.parse_args()


    # Setup logging config
    log_type=opts.log_level
    log.setLevel(log_type.upper())

    #Consume message from RabbitMQ 
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_message(message,opts))

