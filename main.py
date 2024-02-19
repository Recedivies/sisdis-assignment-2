import logging
import multiprocessing
import threading
import random
import socket
import sys
import time
from argparse import ArgumentParser
import node
from ast import literal_eval
from pprint import pformat
from logging.handlers import QueueHandler, QueueListener

# RUN IN PYTHON 3.8.8
main_status_dictionary = {}
node_dictionary = {}


def setup_logger():
    log = logging.getLogger(__name__)
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(
        format="%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s",
        datefmt="%H:%M:%S",
        filename="logs/main.txt",
        filemode="w",
        level=logging.DEBUG,
    )


class NodeProcess(multiprocessing.Process):
    def run(self):
        try:
            super().run()
        except Exception:
            logger.error(f"{self.name} has an error")


def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(
        f"Uncaught exception handler", exc_info=(exc_type, exc_value, exc_traceback)
    )


def thread_exception_handler(args):
    logger.error(
        f"Uncaught exception",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "-n", type=str, dest="node", help="The number of nodes", default="4"
    )
    parser.add_argument(
        "-m",
        type=str,
        dest="neighbors",
        help="The number of chosen random neighbors when a node wants to send a gossip message",
        default=2,
    )
    parser.add_argument(
        "-b",
        type=str,
        dest="heartbeat",
        help="The particular duration of the heartbeat",
        default=2,
    )
    parser.add_argument(
        "-f",
        type=str,
        dest="fault_duration",
        help="The particular duration to assume a node to be a fault",
        default=4,
    )
    parser.add_argument(
        "-p", type=str, dest="port", help="Starting port", default=10000
    )
    parser.add_argument(
        "-d",
        type=str,
        dest="kill_duration",
        help="The particular duration for a node " "to become a fault",
        default=6,
    )
    return parser.parse_args()


def setup_nodes(args):
    sys.excepthook = handle_exception

    logger.info("The main program is running")
    logger.info("Determining the ports that will be used")
    starting_port = random.randint(10000, 11000)
    number_of_nodes = int(args.node)
    port_used = [port for port in range(starting_port, starting_port + number_of_nodes)]
    logger.debug(f"port_used: {port_used}")
    logger.info("Done determining the ports that will be used")

    logger.debug(f"number_of_nodes: {number_of_nodes}")
    logger.debug(f"heartbeat: {float(args.heartbeat)}")
    logger.debug(f"fault_duration: {args.fault_duration}")
    logger.info("Start running multiple nodes")
    for node_id in range(number_of_nodes):
        process = NodeProcess(
            target=node.main,
            name=f"node-{node_id+1}",
            args=(
                float(args.heartbeat),
                int(args.neighbors),
                float(args.fault_duration),
                starting_port + node_id,
                node_id + 1,
                port_used,
                starting_port - 1,
            ),
        )
        process.start()
        main_status_dictionary[f"node-{node_id+1}"] = [0, True]
        node_dictionary[f"node-{node_id+1}"] = process

    logger.info(f"status_dictionary:\n{pformat(main_status_dictionary)}")
    logger.info("Done running multiple nodes")
    logger.debug(f"number of running processes: {len(node_dictionary)}")

    return starting_port, port_used


def listening_procedure(port, timeout, stop_event):
    # TODO
    # Create a socket to listen to the nodes
    # Arguments: Main Node port, timeout duration, threading stop_event
    # See example/main.txt for example output
    """"""


def check_node_status(port):
    # TODO
    # Create a socket to get node status dictionary
    # Arguments: Node port
    # See example/main.txt for example output
    """"""


def shutdown_nodes(kill_duration):
    logger.debug(f"kill_duration: {kill_duration}")
    logger.info("Start stopping the nodes")
    while len(node_dictionary) > 0:
        time.sleep(int(kill_duration))
        node_name, process = node_dictionary.popitem()
        process.kill()
        logger.debug(f"Kill Node with process ID: {node_name}")
    logger.info("Done stopping all the nodes")


def interactive_mode(starting_port, port_used, args):
    while True:
        try:
            print("Enter command:")
            print("1. 'status' to print current status")
            print("2. 'check n' to check node n status")
            print("3. 'start n' to start node n")
            print("4. 'kill n' to kill node n")
            print("5. 'shutdown' to shutdown all nodes")

            command = input().split()

            if command[0] == "status":
                logger.info("Printing status")
                logger.info(f"Status Dictionary:\n{pformat(main_status_dictionary)}")
                print(f"Status Dictionary:\n{pformat(main_status_dictionary)}")
                logger.info(f"Node Dictionary:\n{pformat(node_dictionary)}")
                print(f"Node Dictionary:\n{pformat(node_dictionary)}")
            elif command[0] == "check" and len(command) == 2:
                node_id = int(command[1])
                # TODO
                # Check the Status Dictionary of the node with the given node_id
                # Should print Status Dictionary like status command above

            elif command[0] == "start" and len(command) == 2:
                node_id = int(command[1])
                # TODO
                # Start the node with the given node_id
                # Configuration should match the one in setup_nodes

            elif command[0] == "kill" and len(command) == 2:
                node_id = int(command[1])
                # TODO
                # Kill the node with the given node_id
                # All nodes should mark this node as fault in their Status Dictionary automatically

            elif command[0] == "shutdown":
                print("Shutting down all nodes")
                shutdown_nodes(args.kill_duration)
                break
            else:
                print("Invalid command.")
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"Error: {e}")
            print("Shutting down all nodes")
            shutdown_nodes(args.kill_duration)
            break


def main():
    global logger
    setup_logger()
    logger = logging.getLogger(__name__)
    print("Main Node running")
    logger.info("Main Node running")
    argsval = parse_args()
    logger.info(f"Arguments: {argsval}")

    starting_port, port_used = setup_nodes(argsval)

    stop_event = threading.Event()
    threading.excepthook = thread_exception_handler
    thread = threading.Thread(
        target=listening_procedure,
        args=(
            starting_port - 1,
            argsval.heartbeat + argsval.fault_duration,
            stop_event,
        ),
    )
    thread.name = "main_listening_thread"
    thread.start()
    logger.debug(f"Listener started at port {starting_port-1}")

    interactive_mode(starting_port, port_used, argsval)

    print("All nodes have been stopped. Shutting down Main Node")
    stop_event.set()
    thread.join()
    exit(0)


if __name__ == "__main__":
    main()
