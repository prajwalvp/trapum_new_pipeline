import glob
import optparse
from optparse import OptionParser
import sys
import pika_process
import json
import logging
from datetime import datetime


def receive_message(message,opts):
    global info
    info = json.loads(message.decode("utf=8"))
    print(info)

                
if __name__=='__main__':

    # Update all input arguments
    consume_parser = optparse.OptionParser()
    pika_process.add_pika_process_opts(consume_parser)
    opts,args = consume_parser.parse_args()


    # Setup logging config
    #log_type=opts.log_level
    #log.setLevel(log_type.upper())

    #Consume message from RabbitMQ 
    processor = pika_process.pika_process_from_opts(opts)
    processor.process(lambda message: receive_message(message,opts))

