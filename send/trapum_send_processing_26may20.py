import subprocess
import glob
import optparse
import parseheader
import os
import logging
import sys
import pika_process
from datetime import datetime
import trapum_db_send


log = logging.getLogger('trapum_all_beam_merger')
FORMAT = "[%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
#logging.basicConfig(filename='trapum_beam_merger.log',filemode='a',format=FORMAT,level=logging.DEBUG)
logging.basicConfig(format=FORMAT,level=logging.DEBUG)


def get_no_of_subdirectories_in_path(path): #### Latest structure
    script = 'find %s/* -maxdepth 0 -type d'%path
    beam_list = subprocess.getoutput(script)
    return [os.path.basename(b) for b in beam_list.split('\n')]


def get_beam_list(path):  ### Old structure
    #script = 'ls %s/*_cfbf?????_* | awk -F[_,_] \'{print $3}\' | uniq'%path
    script = 'find %s -name "*cfbf*" | awk -F[_,_] \'{print $3}\' | sort | uniq'%path
    unique_beam_list = subprocess.getoutput(script)
    return unique_beam_list.split('\n')




if __name__=="__main__":

    # Pass arguments
    parser = optparse.OptionParser()
    pika_process.add_pika_producer_opts(parser)
    parser.add_option('--length',type=str,help='Length in seconds to merge files',dest="length",default='full')
    parser.add_option('--observation_path',type=str,help='path to observation files',dest="observation")
    parser.add_option('--merged_file_path',type=str,help='path to merged files',dest='merged_path',default="/beegfs/u/prajwalvp/outer_beam_search/merged_filterbanks")
    parser.add_option('--number_of_beams',type=int,help='Merge first N beams',dest='beam_no',default=288)
    parser.add_option('--remove_beams',type=str,help='remove beam numbers',dest='elim_beam',default='0,1,2,3,4,5,6,7,24,25,46,47,48,278') # Half mass beams of Ter5 obs 1;48 and 278 beegfs
    parser.add_option('--downsample_tfactor',type=int,help='downsample factor in time',dest='time_ds')
    parser.add_option('--downsample_ffactor',type=int,help='downsample factor in frequency',dest='freq_ds',default=1)
    parser.add_option('--notes',type=str,help='unique note marker for processing table',dest='notes',default="outer_beam_search")
    opts,args = parser.parse_args() 
    
    new_paths = [x[0] for x in os.walk(opts.observation)]


    # Setup logging config
    log_type=opts.log_level
    log.setLevel(log_type.upper())


 
    # Create directory
    #try:
    #    new_obs = opts.observation.split('/')[-1]
    #    main_merged_path = opts.merged_path + "/" + new_obs 
    #    subprocess.check_call("mkdir -p %s"%main_merged_path,shell=True)
    #except: 
    #    log.info("Directory already made")
    #    pass


    for new_path in new_paths:
        if not glob.glob(new_path+'/*.meta'):
            log.debug("No meta file found in path %s"%new_path)
            continue
        else:
             # Find total number of beams
            log.info("Beam files found in path %s"%new_path)
            #beam_list = get_beam_list(new_path)
            beam_list = get_no_of_subdirectories_in_path(new_path)
            print(beam_list)    

            log.info("Number of coherent beams found: %d"%len(beam_list))


            # Reduced beam list
            beam_list_mod = beam_list[:opts.beam_no]
            
            # Remove beams not wanted
            elim_beam_list = ['cfbf'+'{0:05d}'.format(int(s)) for s in opts.elim_beam.split(',')]    
            unique_beam_list = [x for x in beam_list_mod if x not in elim_beam_list]

                
            #print(unique_beam_list)
            log.info("Number of beams to process: %d"%len(unique_beam_list))


            # Retrieve partial filename and create subdirectory

            info={}
            for beam_name in unique_beam_list:
                try:
                    subdirectory_name = new_path.split('/')[-2]+'/'+new_path.split('/')[-1]
                    final_merge_path = opts.merged_path + '/' + subdirectory_name +'/' + beam_name
                    subprocess.check_call("mkdir -p %s"%(final_merge_path),shell=True)
                except:
                    log.info("Subdirectory already made")
                    pass

                if opts.length =='full':
                   log.info("Merging all files for the beam")
                   if opts.time_ds:
                       full_combined_path = final_merge_path + '/'+"%s_full_combined_t%d_f%d.fil"%(beam_name,opts.time_ds,opts.freq_ds)
                       digifil_script = "/beegfs/u/prajwalvp/digifil %s/*.fil -threads 15 -b 8 -t %d  -o %s"%(new_path+'/'+beam_name,opts.time_ds,full_combined_path)
                   else: 
                       full_combined_path = final_merge_path + '/'+"%s_full_combined_f%d.fil"%(beam_name,opts.freq_ds)
                       digifil_script = "/beegfs/u/prajwalvp/digifil %s/*.fil -threads 15 -b 8 -o %s"%(new_path+'/'+beam_name,full_combined_path)

                     
                   info['script'] = digifil_script
                   info['subgroup_files'] = subgroup_files
                   info['dp_ids'] = dp_ids
                   info['output_path'] = final_merge_path
                   info['filename'] = "%s_t1_f1_merge_%d.fil"%(beam_name,merges)
             
                   print(info)

                   # Create processing

                   # Get pipeline ID
                   #pipeline_id = trapum.get_pipeline_id_from_name("peasoup32")
   
                   # create Processing

                   #submit_time = str(datetime.now()) 
                   #info["processing_id"] = trapum.create_processing(pipeline_id,submit_time,"enqueued")

                   # Publish to Rabbit
                   #log.debug("Ready to publish to RabbitMQ")
                   #pika_process.publish_info(opts,info)
                   sys.exit(0)




                files_per_beam = sorted(glob.glob(new_path+'/'+beam_name+'/*.fil'))
                
                if len(files_per_beam) ==1:
                    log.info("No need to merge since one file recorded per beam")
                    sys.exit(0)

                
                file_info1 = parseheader.parseSigprocHeader(files_per_beam[0])
                file_info = parseheader.updateHeader(file_info1)



                if float(opts.length)/float(file_info['tobs']) < 2.0 :
                    log.info("No need to merge since required length is smaller than two files")
                    continue
                    #sys.exit(0)
                elif float(opts.length) > len(files_per_beam)*file_info['tobs']:
                    #log.info("Length asked for exceeds total observation length!!")
                    continue
                    #sys.exit(0)
                else:
                    no_of_files_per_merge = int(round(float(opts.length)/file_info['tobs']))
                    log.info("Number of files in merge: %d"%no_of_files_per_merge)
                    print(len(files_per_beam),file_info['tobs'])
                    if len(files_per_beam)%2==0:
                        no_of_merges = int(len(files_per_beam)/no_of_files_per_merge)
                    else:
                        no_of_merges = int((len(files_per_beam)+1)/no_of_files_per_merge)

                    log.info("Number of merges: %d"%no_of_merges)
                     
                    actual_length = no_of_files_per_merge*float(file_info['tobs'])
                    log.info("Closest length to given length: %f"%actual_length)
                   
                    
                    for merges in range(no_of_merges):
                        #print(merges)
                        if merges==no_of_merges-1:
                            #subgroup_files = " ".join(files_per_beam[merges*no_of_files_per_merge:])     
                            subgroup_files = " ".join(files_per_beam[-no_of_files_per_merge:])     
                        else:
                            subgroup_files = " ".join(files_per_beam[merges*no_of_files_per_merge:(merges+1)*no_of_files_per_merge])  

                        # Get respective data product ids for each list of files 
                        #trapum = trapum_db_external.TrapumDataBase();
                        trapum = trapum_db_send.TrapumDataBase("db_host","db_port","db_name","db_user","db_passwd");
                        dp_ids=[]
                        for ind_file in subgroup_files.split(' '):
                            #print (ind_file)
                            dp_ids.append(trapum.get_dp_id_from_filename(os.path.basename(ind_file))[0])                            

                        if opts.time_ds:  
                            digifil_script = "/beegfs/u/prajwalvp/digifil %s -threads 15 -b 8 -t %d  -o %s/%s_t1_f1_merge_%d.fil"%(subgroup_files,opts.time_ds,final_merge_path,beam_name,merges)
                        else:
                            digifil_script = "/beegfs/u/prajwalvp/digifil %s -threads 15 -b 8  -o %s/%s_t1_f1_merge_%d.fil"%(subgroup_files,final_merge_path,beam_name,merges)


                        
                        info['script'] = digifil_script
                        info['subgroup_files'] = subgroup_files
                        info['dp_ids'] = dp_ids
                        info['output_path'] = final_merge_path
                        info['filename'] = "%s_t1_f1_merge_%d.fil"%(beam_name,merges)

                        # Create processing

                        # Get pipeline ID
                        pipeline_id = trapum.get_pipeline_id_from_name("peasoup32")
   
                        # create Processing

                        submit_time = str(datetime.now()) 
                        info["processing_id"] = trapum.create_processing(pipeline_id,submit_time,"enqueued",opts.notes)

                        #print processing_id

                        print(info)

                        # Publish to Rabbit
                        log.debug("Ready to publish to RabbitMQ")
                        pika_process.publish_info(opts,info)

                           
