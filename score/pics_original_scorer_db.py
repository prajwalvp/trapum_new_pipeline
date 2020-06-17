import sys
sys.path.append('/home/psr')
import cPickle, glob, ubc_AI
from ubc_AI.data import pfdreader
import optparse
import logging
import trapum_db_score
import tarfile
import os
import pika_process
import json
from datetime import datetime
import subprocess

log = logging.getLogger('pics_scorer')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)

AI_PATH = '/'.join(ubc_AI.__file__.split('/')[:-1])

def make_tarfile(output_path,input_path,name):
    with tarfile.open(output_path+'/'+name, "w:gz") as tar:
        tar.add(input_path, arcname= name)


def extract_and_score(opts,info):
    # Load model
    classifier = cPickle.load(open(AI_PATH+'/trained_AI/%s'%info["model"],'rb'))
    log.info("Loaded model %s"%info["model"])

    # Find all files
    pfdfile = glob.glob('%s/*.pfd'%(info["input_path"]))
    log.info("Retrieved pfd files from %s"%(info["input_path"]))
        
    #Setup database connection
    log.debug("Connecting to TrapumDB...")
    try:
        trapum = trapum_db_score.TrapumDataBase("db_host_ip","db_port","db_name","user","passwd");
        t =trapum.connect()
    except:
        log.error("DB connection failed")
        pass
    log.debug("Connected to TrapumDB")

    #Get pipeline ID 
    pipeline_id = trapum.get_pipeline_id_from_name("PICS_Original")

    # Create Processing
    #submit_time = str(datetime.now())
    #processing_id = trapum.create_processing(pipeline_id,submit_time,"queued")

    # Get processing
    processing_id = info["processing_id"]

    # Create_processing_pivot_entries
    dp_id = info["dp_id"]
    pp_id = trapum.create_pivot(dp_id,processing_id)

    # Start time update
    start_time = str(datetime.now())
    trapum.update_start_time(start_time,processing_id)


    AI_scores = classifier.report_score([pfdreader(f) for f in pfdfile])
    log.info("Scored with model %s"%info["model"])

    # Sort based on highest score
    pfdfile_sorted = [x for _,x in sorted(zip(AI_scores,pfdfile),reverse=True)]
    AI_scores_sorted = sorted(AI_scores,reverse=True)
    log.info("Sorted scores..")

    text = '\n'.join(['%s %s' % (pfdfile_sorted[i], AI_scores_sorted[i]) for i in range(len(pfdfile))])

    fout = open('%s/pics_original_descending_scores.txt'%info["input_path"],'w')
    fout.write(text)
    log.info("Written to file in %s"%info["input_path"])
    fout.close()

    # End time update
    end_time = str(datetime.now())
    trapum.update_end_time(end_time,processing_id)
    trapum.update_processing_status("Successful",processing_id)


    #tar all files in this directory
    tar_name = os.path.basename(info["input_path"])+"_presto_cands.tar"
    make_tarfile(info["input_path"],info["input_path"],tar_name)

    
    # Get pointing and beam id from one of the dp_ids
    try:
        pb_list = trapum.get_pb_from_dp(dp_id)
    except Exception as error:
        log.error(error)


    # Update Dataproducts with pics output entry and tarred files
    dp_id = trapum.create_secondary_dataproduct(pb_list[0],pb_list[1],processing_id,1,"pics_original_descending_scores.txt",str(info["input_path"]),12)
    dp_id = trapum.create_secondary_dataproduct(pb_list[0],pb_list[1],processing_id,1,str(tar_name),str(info["input_path"]),11)


    # Remove original files 
    subprocess.check_call("rm *pfd* *.txt",shell=True,cwd=str(info["input_path"]))
  

def receive_message(message,opts):
    global info
    info = json.loads(message.decode("utf=8"))
    extract_and_score(opts,info)





if __name__=='__main__':


    # Update all input arguments
    parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(parser)


    #parser.add_option('--in_model',type=str,help='input model name',dest="model",default="clfl2_PALFA.pkl")
    #parser.add_option('--file_type',type=str,help='file extension (pfd/ar/ar2)',dest="type",default="pfd")
    #parser.add_option('--in_path',type=str,help='input path for files to be scored',dest="in_path")
    #parser.add_option('--out_path',type=str,help='output path for scores',dest="out_path")
    opts,args = parser.parse_args()
 
    #Configure log setup
    log_type = opts.log_level
    log.setLevel(log_type.upper())


    #Consume message from RabbitMQ 
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_message(message,opts))

