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

def consume_from_rabbit(opts):
    # Consume message from RabbitMQ
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_and_make_peasoup_args(message,opts))

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
    iqr_script = "iqrm_apollo_cli -m 3 -t 6.0 -i %s -s 10000 -f 4096 -o %s"%(merged_file,iqr_file)

    try:
        subprocess.check_call(iqr_script,shell=True)
        log.info("IQR filtering done on %s"%merged_file)   
    except Exception as error:
        log.error(error)

    info["iqr_file"] = iqr_file
    return info 

def subband(info,opts):
    info['subband_file'] = info['iqr_file'][:-4]+'_subbanded.fil'
    subband_script="ft_scrunch_threads -d 240 -t 16 -B 64 -c 16 --no-refdm %s %s"%(info['iqr_file'],info['subband_file'])
    try:
        subprocess.check_call(subband_script,shell=True)
        log.info("Subbanding done")
        return info 
    except Exception as error:
        log.error(error)
    
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



def test_no_cands(opts):

    # Check number of cands created
    no_of_cands  = subprocess.getoutput("sed -n \'/<candidates>/,/<\/candidates>/p\' %s | wc -l"%opts.xml)
    if (int(no_of_cands))== 9:
        log.info("No candidates generated to fold. Relaunching back to queue...")
        info={}
        info['failed'] = "yes"  
        #Publish back to queue   
        opts.queue = opts.input_queue   
        pika_process.publish_info(opts,info)
        # Consume next message
        consume_from_rabbit(opts)
             
   
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




    
   
if __name__=='__main__':
   
    # Update all input arguments
    parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(parser)
    parser.add_option('--pl',type=str,help='pipeline details file',dest="pipe")
    parser.add_option('--xml',type=str,help='xml file to test',dest="xml")
    parser.add_option('--out',type=str,help='output directory name',dest="outp",default="/beegfs/PROCESSING/TRAPUM/peasoup32_products")
    parser.add_option('--segment_lengths',type=str,help='segment lengths to search over full file',dest="segs",default="10,20")
    parser.add_option('--dm_segments',type=int,help='number of dm segments (RAM limited)',dest="dm_segs",default=2)
    parser.add_option('--companion_mass',type=float,help='Companion mass in solar masses',dest="mc",default=4.0)
    opts,args = parser.parse_args()

    # Setup logging config
    log_type = opts.log_level
    log.setLevel(log_type.upper())


    # Test no cands
    test_no_cands(opts)
   



