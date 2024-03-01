import logging
from math import log
import random
import threading
import time
import socket
from ast import literal_eval
from pprint import pformat

timer_dict = {}


def start_fault_timer(key):
    status_dictionary[key][1] = False
    logger.info(f"This node become a fault: {key}")
    logger.info(f"Node fault status_dictionary:\n{pformat(status_dictionary)}")


def send_message(node_id, port):
    logger.debug("Create the client socket")
    client_socket = socket.socket(
        socket.AF_INET, socket.SOCK_DGRAM
    )  # Note that this socket is UDP
    logger.debug("Encode the message")
    message = f"node-{node_id}#{status_dictionary}"
    logger.debug(f"message: {message}")
    message = message.encode("UTF-8")
    addr = ("127.0.0.1", port)
    logger.debug("Send the message")
    client_socket.sendto(message, addr)


def sending_procedure(
    heartbeat_duration,
    node_id,
    neighbors_ports,
    node_ports,
    main_port,
    num_of_neighbors_to_choose,
):
    # TODO
    # Create a socket to send the heartbeat to the neighbors and main node
    # Arguments: Heartbeat duration, this Node ID, Neighbors port, Node ports, Main Node port
    # Note: use send_message function to send the heartbeat

    while True:
        # Increase heartbeat node-i
        status_dictionary[f"node-{node_id}"][0] += 1
        logger.info(f"Increase heartbeat node-{node_id}:\n{pformat(status_dictionary)}")

        logger.info("Determining which node to send...")

        random_neighbors = random.sample(neighbors_ports, num_of_neighbors_to_choose)

        node_names = [f"node-{node_ports[node]}" for node in random_neighbors]
        log_message = "Send messages to main node, " + ", ".join(node_names)

        logger.info(log_message)

        send_message(node_id=node_id, port=main_port)
        for neighbor_port in random_neighbors:
            send_message(node_id=node_id, port=neighbor_port)

        time.sleep(heartbeat_duration)


def fault_timer_procedure(node_id, fault_duration):
    for key in status_dictionary.keys():
        logger.debug(f"key: {key}")
        if key == node_id:
            continue
        thread = threading.Timer(fault_duration, start_fault_timer, (key,))
        thread.start()
        timer_dict[key] = thread


def tcp_listening_procedure(port, node_id):
    # TODO
    # Create a TCP socket to listen to the main node
    # Arguments: Node port, Node ID

    logger.info("Initiating TCP socket")

    tcp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_client_socket.bind(("127.0.0.1", port))
    tcp_client_socket.listen(1)

    logger.info("Listening for incoming TCP connections...")

    while True:
        connection, address = tcp_client_socket.accept()
        logger.info(f"Connected by {address}")

        input_value = connection.recv(1024).decode("UTF-8")
        logger.info("Received status request, sending status data...")

        message = f"{status_dictionary}"
        message = message.encode("UTF-8")

        connection.send(message)

        connection.close()


def is_restart_node() -> bool:
    cnt = 0
    for node_id in range(1, len(status_dictionary) + 1):
        cnt += status_dictionary[f"node-{node_id}"][0]
    return cnt == 1


def listening_procedure(port, node_id, fault_duration):
    # TODO
    # Create a UDP socket to listen to all other node
    # Arguments: This Node's port, Node ID, fault duration

    logger.info("Start the timer for fault duration...")

    fault_timer_procedure(node_id=f"node-{node_id}", fault_duration=fault_duration)

    logger.info("Initiating socket...")

    udp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_client_socket.bind(("127.0.0.1", port))

    logger.info("Listen for incoming messages...")

    while True:
        input_value_byte, address = udp_client_socket.recvfrom(1024)
        message_raw = input_value_byte.decode("UTF-8")

        logger.info(f"message_raw: {message_raw}")

        splitted_message = message_raw.split("#")
        sender_node = splitted_message[0]
        status_dictionary_node = splitted_message[1]

        if sender_node in timer_dict:
            timer_dict[sender_node].cancel()
            thread = threading.Timer(fault_duration, start_fault_timer, (sender_node,))
            thread.start()
            timer_dict[sender_node] = thread

        dict_status_dictionary = literal_eval(status_dictionary_node)

        logger.info(f"Receive message from {sender_node}...")

        logger.info(f"Incoming message:\n{pformat(dict_status_dictionary)}")

        logger.debug(f"address: {address}")
        logger.debug("Process the message...")

        for node_id in range(1, len(status_dictionary) + 1):
            node = f"node-{node_id}"

            current_logical_time = status_dictionary[node][0]
            current_status = status_dictionary[node][1]

            input_logical_time = dict_status_dictionary[node][0]
            input_status = dict_status_dictionary[node][1]

            logger.debug(f"input_node_list {node}: {dict_status_dictionary[node]}")
            logger.debug(f"current_node_list {node}: {status_dictionary[node]}")

            if (
                current_logical_time >= input_logical_time
                and current_status == input_status
                and not is_restart_node()
                and input_logical_time != 1
            ):
                logger.debug("Skip this loop...")
                continue

            logger.debug(
                f"Update logical time {node}: {current_logical_time} -> {input_logical_time}"
            )
            status_dictionary[node][0] = input_logical_time
            status_dictionary[node][1] = input_status

            logger.debug(f"Check if {node} has died...")

            if input_status == True:
                logger.debug(f"{node} has not died...")
            else:
                logger.debug(f"{node} has died...")

        logger.info(
            f"status_dictionary after incoming message:\n{pformat(status_dictionary)}"
        )


def handle_exception(exc_type, exc_value, exc_traceback):
    logger.error(
        f"Uncaught exception handler", exc_info=(exc_type, exc_value, exc_traceback)
    )


def thread_exception_handler(args):
    logger.error(
        f"Uncaught exception",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(
        format="%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s",
        datefmt="%H:%M:%S",
        filename=filename,
        filemode="w",
        level=logging.DEBUG,
    )


def main(
    heartbeat_duration=1,
    num_of_neighbors_to_choose=1,
    fault_duration=1,
    port=1000,
    node_id=1,
    neighbors_ports=[1000, 1001],
    main_port=999,
):
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
            node_ports[neighbors_ports[i]] = i + 1
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
        thread = threading.Thread(
            target=listening_procedure, args=(port, node_id, fault_duration)
        )
        thread.name = "listening_thread"
        thread.start()

        logger.info("Executing the sending procedure")
        thread = threading.Thread(
            target=sending_procedure,
            args=(
                heartbeat_duration,
                node_id,
                neighbors_ports,
                node_ports,
                main_port,
                num_of_neighbors_to_choose,
            ),
        )
        thread.name = "sending_thread"
        thread.start()
    except Exception as e:
        logger.exception("Caught Error")
        raise


if __name__ == "__main__":
    main()
