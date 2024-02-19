import logging
from math import log
import random
import threading
import time
import socket
from ast import literal_eval
from pprint import pformat

def start_fault_timer(key):
    status_dictionary[key][1] = False
    logger.info(f"This node become a fault: {key}")
    logger.info(f"Node fault status_dictionary:\n{pformat(status_dictionary)}")

def send_message(node_id, port):
    logger.debug("Create the client socket")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # Note that this socket is UDP
    logger.debug("Encode the message")
    message = f"node-{node_id}#{status_dictionary}"
    logger.debug(f"message: {message}")
    message = message.encode("UTF-8")
    addr = ("127.0.0.1", port)
    logger.debug("Send the message")
    client_socket.sendto(message, addr)

def sending_procedure(heartbeat_duration, node_id, neighbors_port, node_ports, main_port):
    logger.info("Sending Procedure Thread Created")
    # TODO
    # Create a socket to send the heartbeat to the neighbors and main node
    # Arguments: Heartbeat duration, this Node ID, Neighbors port, Node ports, Main Node port
    # Note: use send_message function to send the heartbeat
    ""

def fault_timer_procedure(node_id, fault_duration):
    for key in status_dictionary.keys():
        logger.debug(f"key: {key}")
        if key == node_id:
            continue
        thread = threading.Timer(fault_duration, start_fault_timer, (key,))
        thread.start()
        
def tcp_listening_procedure(port, node_id):
    logger.info("Initiating TCP socket")
    # TODO
    # Create a TCP socket to listen to the main node
    # Arguments: Main Node port, Node ID
    ""
                    
def listening_procedure(port, node_id, fault_duration):
    # TODO
    # Create a UDP socket to listen to all other node
    # Arguments: This Node's port, Node ID, fault duration
    ""

def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(f"Uncaught exception handler", exc_info=(exc_type, exc_value, exc_traceback))

def thread_exception_handler(args):
    logger.error(f"Uncaught exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.DEBUG)

def main(heartbeat_duration=1, num_of_neighbors_to_choose=1,
         fault_duration=1, port=1000, node_id=1, neighbors_ports=[1000,1001], main_port=999):
    reload_logging_windows(f"logs/node{node_id}.txt")
    global logger
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"Node with id {node_id} is running")
        logger.debug(f"heartbeat_duration: {heartbeat_duration}")
        logger.debug(f"fault_duration: {fault_duration}")
        logger.debug(f"port: {port}")
        logger.debug(f"num_of_neighbors_to_choose: {num_of_neighbors_to_choose}")
        logger.debug(f"initial neighbors_ports: {neighbors_ports}")

        logger.info("Configure the status_dictionary global variable")
        global status_dictionary
        status_dictionary = {}
        node_ports = {}
        for i in range(len(neighbors_ports)):
            status_dictionary[f"node-{i + 1}"] = [0, True]
            node_ports[neighbors_ports[i]] = i+1
        neighbors_ports.remove(port)
        logger.debug(f"final neighbors_ports: {neighbors_ports}")
        logger.info(f"status_dictionary:\n{pformat(status_dictionary)}")
        logger.info("Done configuring the status_dictionary")

        logger.info("Executing the status check procedure")
        thread = threading.Thread(target=tcp_listening_procedure, args=(port, node_id))
        thread.name = "tcp_listening_thread"
        thread.start()

        logger.info("Executing the listening procedure")
        threading.excepthook = thread_exception_handler
        thread = threading.Thread(target=listening_procedure, args=(port, node_id, fault_duration))
        thread.name = "listening_thread"
        thread.start()
        
        logger.info("Executing the sending procedure")
        thread = threading.Thread(target=sending_procedure,
                         args=(heartbeat_duration,
                               node_id, neighbors_ports, node_ports, main_port))
        thread.name = "sending_thread"
        thread.start()
    except Exception as e:
        logger.exception("Caught Error")
        raise

if __name__ == '__main__':
    main()
