import optparse
import pika_process
import parseheader
import json
import logging
import trapum_db_search
import math
import glob
import os
import string
from sigpyproc.Readers import FilReader
import subprocess
from datetime import datetime
import time



log = logging.getLogger('peasoup_search_send')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

#constants##
T = 4.925e-6 # in seconds
c = 2.99792458e8 # in m/s


def mass_function(mp,mc):
    return (mc**3)/((mp + mc)**2)


def calculate_amax(p_orb,mc):
    mf = mass_function(1.4,mc)
    p_orb_s = p_orb*3600
    return ((2*math.pi/p_orb_s)**(1.333333))*((T*mf)**(0.333333))*c

def remove_username(xml_file):
    script = "sed -i \'/username/d\' %s"%xml_file
    subprocess.check_call(script,shell=True)

def iqr_filter(info,opts):
    merged_file = info["script"].split("-o")[1].strip()
    iqr_file = "%s_iqr.fil"%merged_file[:-4]
    iqr_script = "iqrm_apollo_cli -m 3 -t 6.0 -i %s -s 100000 -f 4096 -o %s"%(merged_file,iqr_file)

    try:
        subprocess.check_call(iqr_script,shell=True)
        log.info("IQR filtering done on %s"%merged_file)   
    except Exception as error:
        log.error(error)

    info["iqr_file"] = iqr_file
    return info 

def merge_filterbanks(info,opts):
    digifil_script = info['script']
    print (digifil_script)

    try:
        subprocess.check_call(digifil_script,shell=True)
        log.info("Successfully merged.Send file to IQR filter")
         

    except subprocess.CalledProcessError:
        log.info("Partial file exists. Removing and creating a new one....")   
        subprocess.check_call("rm %s/%s"%(info['output_path'],info['filename']),shell=True)
        print ("rm %s/%s"%(info['output_path'],info['filename']))
        subprocess.check_call(digifil_script,shell=True)
 
    except Exception as error:
        log.error(error)    


def split_filterbank(opts,info):

     filename = info['filename']
     segments = info["segments"]
     output_location = info["basename"]

     segment_lengths = map(float, segments.split(','))
     for segment_length in segment_lengths:

         
         obs_length_req = segment_length*60.0
         input_file = FilReader(filename)
         number_samples = input_file.header.nsamples
         obs_time_seconds = input_file.header.tobs
 
         if obs_length_req > obs_time_seconds:
             log.info("Segment length exceeds observation file length. Skipping the split..")    
             continue

         log.info("calculate acceleration range for %d min"%segment_length)
         pb = 10.0*segment_length/(60.0)  # 10 percent rule 
         amax = calculate_amax(pb,info["mc"])
         info['start_acc'] = -1*amax
         info['end_acc'] = amax

         sampling_time = input_file.header.tsamp
         NTOREAD = int(obs_length_req/sampling_time)
         number_of_fils= int(obs_time_seconds/obs_length_req)

         for i in range(number_of_fils+1):
             START = i*NTOREAD
             path = output_location + '_segment_%d_length_%dmin'%(i,segment_length) + '.fil'
             output_name = os.path.basename(path)[:-4]

             if i==number_of_fils:
                 log.info("Last split")
                 input_file.split(number_samples-NTOREAD,NTOREAD, filename = path)                 
                 info["output_name"] = output_name
                 info["input_name"] = path
                 call_peasoup(info)
                 log.info("Segmented search done!")

             else:
                 log.info("Split %d"%i)
                 log.info('START at sample: %d'%START)
                 input_file.split(START,NTOREAD, filename = path)
                 info["output_name"] = output_name
                 info["input_name"] = path
                 call_peasoup(info)



def call_peasoup(opts,info):

    o_path = info['output_path']+ '/' + info['output_name'] + '_dm_'+ str(info['start_dm']) + '_' + str(info['end_dm'])

    #peasoup_script = "numactl --preferred 1 taskset -c 12-23 peasoup -i %s --dm_start %s --dm_end %s --acc_start %s --acc_end %s -k %s -z %s -m %s --fft_size %s --limit %s -o %s"%(info['input_name'],info['start_dm'],info['end_dm'],info['start_acc'],info['end_acc'],info['mask_file'],info['birdie_file'],info['snr_threshold'],
    peasoup_script = "peasoup -i %s --dm_start %s --dm_end %s --acc_start %s --acc_end %s -k %s -z %s -m %s --fft_size %d --limit %s -o %s"%(info['input_name'],info['start_dm'],info['end_dm'],info['start_acc'],info['end_acc'],info['mask_file'],info['birdie_file'],info['snr_threshold'],
                      int(info['fft_size']),info['limit_on_candidates'],o_path)

    #print(info)

    log.debug("Connecting to TrapumDB...")
    try:
        trapum = trapum_db_search.TrapumDataBase("db_ip","db_port","db_name","db_user","db_passwd");
        con = trapum.connect()
        log.info("Connected to TrapumDB")
    except:
        pass    # !!! Need to decide what to do when DB fails


    processing_id = info["processing_id"]

    
    # Create_processing_pivot_entries
    dp_ids = info["dp_ids"]
    pp_ids=[]
    for dp_id in dp_ids:
        pp_id = trapum.create_pivot(dp_id,processing_id)
        pp_ids.append(pp_id)

    # Start time update to db, change process status to searching...
    log.info("Starting peasoup search..")
    print(peasoup_script)
    try:
        start_time = str(datetime.now())
        trapum.update_start_time(start_time,processing_id)
        subprocess.check_call(peasoup_script,shell=True)
        end_time = str(datetime.now())
        trapum.update_processing_status("Successful",processing_id)
        trapum.update_end_time(end_time,processing_id)
                
    except Exception as error:
        #time.sleep(100)
        trapum.update_processing_status("Failed",processing_id)
        trapum.update_processing_notes(str(error).replace("'",""),processing_id)
        log.error(error)

    # Get pointing and beam id from one of the dp_ids
    try:
        pb_list = trapum.get_pb_from_dp(dp_id)
    except:
        time.sleep(100)

    # Update Dataproducts with peasoup output entry
    dp_id = trapum.create_secondary_dataproduct(pb_list[0],pb_list[1],processing_id,1,"overview.xml",str(o_path),8)


    # Create a folding processing 

    # Get pipeline ID
    pipeline_id = trapum.get_pipeline_id_from_name("PRESTO")

    # create Processing

    submit_time = str(datetime.now())
    fold_processing_id = trapum.create_processing(pipeline_id,submit_time,"enqueued")


    # Create folding packet to publish
    folding_info={}
    folding_info['subgroup_files'] = info['subgroup_files']
    print (info["dp_ids"])
    folding_info["dp_ids"] = info['dp_ids']
    folding_info["processing_id"] = fold_processing_id
    folding_info["xml_path"] = o_path
    folding_info["output_path"] = o_path.replace("peasoup32_products","folded_products")
    folding_info["batch_number"] = 15
    folding_info["sub_ints"] = 64
    folding_info["bins"] = 128

    # Remove binary username from xml
    log.debug("Removing username of xml file in %s"%folding_info['xml_path'])
    xml_file = folding_info['xml_path']+str('/overview.xml')
    remove_username(xml_file)
    log.info("Removed username from xml file in %s"%folding_info['xml_path'])


    # Remove merged file and iqr file 
    log.info("Removing merged file %s and the iqr file"%info['input_name'] )
    try:
        if '/beegfs/DATA' not in info['input_name']:    
            subprocess.check_call("rm %s"%(info['input_name']),shell=True)
            merged_file = info["script"].split("-o")[1].strip()
            subprocess.check_call("rm %s"%(merged_file),shell=True)

    except Exception as error:
        log.error(error)


    # Publish to Rabbit
    log.debug("Ready to publish to RabbitMQ")
    opts.queue = "presto_folding_Ter5_all_beams"
    pika_process.publish_info(opts,folding_info)
   
    # end time update to db, change process status to search done and to be folded
    #log.info("peasoup search complete for %s"%info["input_name"])


def get_fil_dict(input_file):
    filterbank_info = parseheader.parseSigprocHeader(input_file)
    filterbank_stats = parseheader.updateHeader(filterbank_info)
 
    return filterbank_stats

def pipeline_dict(opts):
    input_file = opts.pipe

    pipeline = {}
    with open(input_file) as f:
        header = f.readline()
        for line in f:
            (key, val) = line.split(": ")
            val = val.strip("\n")
            pipeline[key] = val

    return pipeline

def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def decide_dm_and_splits(info,opts):
  
    # Split dm_ranges 
    if info['filelen']/(1024*1024*1024) > 10.0 :   # Just put a condition to ensure DMs are split be default
        log.info("File size is greater than third the RAM.. splitting the DM ranges")

        range_val = (float(info['end_dm']) - float(info['start_dm']))/float(info['dm_segs'])    
        dm_start = float(info['start_dm'])
        dm_end = float(info['end_dm'])

        for i in range(info['dm_segs']):
            print('DM split')
            if i== info['dm_segs']-1:
                info['start_dm'] = dm_end - range_val
                info['end_dm'] = dm_end
            else:
                info['start_dm'] = dm_start + i*range_val
                info['end_dm'] = dm_start + (i+1)*range_val

            call_peasoup(opts,info)
            #log.info("Splitting up filterbank into segments of %s min"%info["segments"])
            #split_filterbank(opts,info)
        
    else:
        call_peasoup(opts,info)
        ### !!!  IGNORE segmented search and DM splitting for now
        #log.info("Splitting up filterbank into segments of %s min"%info["segments"])
        #split_filterbank(opts,info2)
        


def receive_and_make_peasoup_args(message,opts):

    #global info
    info = json.loads(message.decode("utf=8"))

    # Merge filterbanks
    merge_filterbanks(info,opts)

    # Apply IQR
    info = iqr_filter(info,opts)

     
    try:
        info['input_name'] = info['input_path']
    except KeyError:
        info['input_name'] = info['iqr_file']

    info['output_name'] = info['filename'][:-4]

    # Get peasoup pipeline requirements from filename
    log.debug("Retrieving peasoup metadata from %s"%opts.pipe)
    pipeline_info = pipeline_dict(opts)
    print(pipeline_info)
    log.info("Retrieved peasoup metadata from %s"%opts.pipe)

    #Get input filterbank metadata
    log.debug("Retrieve header info for %s"%info['input_name'])
    filterbank_info = get_fil_dict(info['input_name'])
    log.info("Header info retrieved for %s"%info['input_name'])
 
    # Merge the two dictionaries
    input_info = merge_two_dicts(pipeline_info,filterbank_info)
    peasoup_info = merge_two_dicts(info,input_info)

    # Make the output directory
    peasoup_info['output_path'] = peasoup_info['output_path'].replace("merged_filterbanks","peasoup32_products")
    try:
        subprocess.check_call("mkdir -p %s"%peasoup_info['output_path'],shell=True)
    except:
        log.info("Subdirectory already made")
        pass


    
    # Check if acceleration range given; if not then  decide it from orbital period and  mass of  companion
    try:
        peasoup_info['start_acc']
    except KeyError:
        log.info("Setting acceleration range based on min. orbital period and companion mass...")
        pb = 10.0*info["tobs"]/(3600.0)  # 10 percent rule 
        amax = calculate_amax(pb,opts.mc)
        peasoup_info['start_acc'] = -1*amax
        peasoup_info['end_acc'] = amax
        peasoup_info["mc"] = opts.mc
   
    #Decide fft_size from filterbank nsamples
    log.debug("Deciding FFT length...")
    bit_length = int(filterbank_info['nsamples']).bit_length()
    if 2**bit_length!=2*int(filterbank_info['nsamples']):
        peasoup_info['fft_size']=2**bit_length
    else:
        peasoup_info['fft_size']=int(info['nsamples'])
    log.info("FFT length set to %s"%peasoup_info['fft_size'])  

    #Save segment lengths in info packet
    peasoup_info["segments"] = opts.segs
    peasoup_info["dm_segs"] = opts.dm_segs

    log.info("Segmented search will be done on %s min segments"%peasoup_info["segments"])
    decide_dm_and_splits(peasoup_info,opts)

    
   
if __name__=='__main__':
   
    # Update all input arguments
    parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(parser)
    parser.add_option('--pl',type=str,help='pipeline details file',dest="pipe")
    parser.add_option('--out',type=str,help='output directory name',dest="outp",default="/beegfs/u/prajwalvp/outer_beam_search/peasoup32_products")
    parser.add_option('--segment_lengths',type=str,help='segment lengths to search over full file',dest="segs",default="10,20")
    parser.add_option('--dm_segments',type=int,help='number of dm segments (RAM limited)',dest="dm_segs",default=10)
    parser.add_option('--companion_mass',type=float,help='Companion mass in solar masses',dest="mc",default=4.0)
    opts,args = parser.parse_args()

    # Setup logging config
    log_type = opts.log_level
    log.setLevel(log_type.upper())

    # Consume message from RabbitMQ
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_and_make_peasoup_args(message,opts))

