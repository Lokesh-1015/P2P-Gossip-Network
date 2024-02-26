import socket
import threading
import logging

logging.basicConfig(filename='seed_network.log', format='%(asctime)s - %(message)s')
network_logger = logging.getLogger()
network_logger.setLevel(logging.DEBUG)

def manage_connection(connection, address):
    while True:
        try:
            data = connection.recv(1024).decode('utf-8')
            if data:
                if "Disconnected Node" in data[:15]:
                    print(data)
                    network_logger.info(data)
                    data = data.split(":")
                    disconnected_node = str(data[1]) + ":" + str(data[2])
                    if disconnected_node in peers:
                        peers.remove(disconnected_node)
                else:
                    data = data.split(":")
                    peers.append(str(address[0]) + ":" + str(data[1]))
                    message = "Connected to " + str(address[0]) + ":" + str(data[1])
                    print(message)
                    network_logger.info(message)
                    peer_list_str = ","
                    for peer in peers:
                        peer_list_str += peer + ","
                    connection.send(peer_list_str.encode('utf-8'))
        except Exception as e:
            print("Error occurred:", e)
            break
    connection.close()

my_ip = "172.31.57.82"
port = int(input("Enter port: "))
peers = []

with socket.socket() as network_socket:
    network_socket.bind((my_ip, port))
    network_socket.listen(5)

    print("Seed Node Listening .. . . .. ")

    while True:
        connection, address = network_socket.accept()
        network_socket.setblocking(1)
        communication_thread = threading.Thread(target=manage_connection, args=(connection, address))
        communication_thread.start()
