import socket
import threading
import time
import hashlib 
import random
import logging
from queue import Queue   

num_threads = 3             
job_numbers = [1, 2, 3]            
job_queue = Queue()               

my_ip_address = "172.31.57.82"                                          
my_port = int(input())                                        
seeds_addresses = set()                                         
peers_from_seed = set()                                 
connected_peers = []                                      
MessageList = []                                          
connect_seed_addr = []      


logging.basicConfig(filename='peer_network.log', format='%(asctime)s - %(message)s')
network_logger = logging.getLogger()
network_logger.setLevel(logging.DEBUG)

class Peer: 
    i = 0
    address = ""
    def __init__(self, addr): 
        self.address = addr 

def peer_to_peer_connection(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')
            received_data = message
            
            if message:
                message_parts = message.split(":")
                message_type = message_parts[0]
                
                if "New Connect Request From" in message_type:
                    new_connection(conn, addr, message_parts)
                elif "Liveness Request" in message_type:
                    liveness_reply(conn, message_parts)
                elif "GOSSIP" in message_parts[3][0:6]:
                    forward_gossip_message(received_data)
        except Exception as e:
            print("Exception occurred:", e)
            break
    
    conn.close()

def new_connection(conn, addr, message_parts):
    if len(connected_peers) < 4:
        conn.send("New Connect Accepted".encode('utf-8'))
        connected_peers.append(Peer(str(addr[0]) + ":" + str(message_parts[2])))

def liveness_reply(conn, message_parts):
    liveness_reply_msg = "Liveness Reply:" + ":".join(message_parts[1:4]) + ":" + str(my_ip_address)
    conn.send(liveness_reply_msg.encode('utf-8'))


def start_connection():
    sock.listen(5)
    print("Peer is Listening")
    while True:
        conn, addr = sock.accept()
        sock.setblocking(1)
        thread = threading.Thread(target = peer_to_peer_connection, args = (conn,addr))
        thread.start()
        

def start_peer_connection(complete_peer_list, selected_peer_nodes_index):
    for i in selected_peer_nodes_index:
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            peer_addr = complete_peer_list[i].split(":")
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            sock.connect(ADDRESS)
            connected_peers.append( Peer(complete_peer_list[i]) )
            message = "New Connect Request From:"+str(my_ip_address)+":"+str(my_port)
            sock.send(message.encode('utf-8'))
            print(sock.recv(1024).decode('utf-8'))
            sock.close()
        except:
            print("Peer Connection Error")

def limit_connection(complete_peer_list):
    if len(complete_peer_list) > 0:
        limit = min(random.randint(1, len(complete_peer_list)), 4)    
        selected_peer_nodes_index = random.sample(range(len(complete_peer_list)), limit)
        start_peer_connection(complete_peer_list, selected_peer_nodes_index)

def union_peer_lists(complete_peer_list):
    global my_ip_address
    complete_peer_list = complete_peer_list.split(",")
    complete_peer_list.pop()
    temp = complete_peer_list.pop()
    temp = temp.split(":")
    my_ip_address = temp[0]
    for i in complete_peer_list:
        if i:
            peers_from_seed.add(i)
    complete_peer_list = list(peers_from_seed)
    return complete_peer_list

def connect_seeds():
    for i in range(0, len(connect_seed_addr)):
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_addr = connect_seed_addr[i].split(":")
            ADDRESS = (str(seed_addr[0]), int(seed_addr[1]))
            sock.connect(ADDRESS)
            MY_ADDRESS = str(my_ip_address)+":"+str(my_port)
            sock.send(MY_ADDRESS.encode('utf-8'))
            message = sock.recv(10240).decode('utf-8')
            complete_peer_list = union_peer_lists(message)
            for peer in complete_peer_list:
                print(peer)
                network_logger.info(peer)
            sock.close()
        except:
            print("Seed Connection Error")
    limit_connection(complete_peer_list)

def register_with_k_seeds():   
    global seeds_addresses
    seeds_addresses = list(seeds_addresses)
    seed_nodes_index = set(random.sample(range(0, n), n // 2 + 1))
    seed_nodes_index = list(seed_nodes_index)
    for i in seed_nodes_index:
        connect_seed_addr.append(seeds_addresses[i])
    connect_seeds()

def report_dead(peer):
    dead_message = "Dead Node:" + peer + ":" + str(time.time()) + ":" + str(my_ip_address)
    print(dead_message)
    network_logger.info(dead_message)
    for seed in connect_seed_addr:        
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            seed_address = seed.split(":")
            ADDRESS = (str(seed_address[0]), int(seed_address[1]))
            sock.connect(ADDRESS)
            sock.send(dead_message.encode('utf-8'))
            sock.close()
        except:
            print("Seed Down ", seed)

def liveness_test():    
    while True:
        liveness_request = "Liveness Request:" + str(time.time()) + ":" + str(my_ip_address)
        print(liveness_request)
        for peer in connected_peers:
            try:                
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(liveness_request.encode('utf-8'))
                print(sock.recv(1024).decode('utf-8'))
                sock.close()  
                peer.i = 0           
            except:                     
                peer.i = peer.i + 1   
                if(peer.i == 3):                
                    report_dead(peer.address)
                    connected_peers.remove(peer)
        time.sleep(13)

def forward_gossip_message(received_message):
    hash_value = hashlib.sha256(received_message.encode()).hexdigest()
    if hash_value in MessageList:        
        pass
    else:
        MessageList.append(str(hash_value))   
        print(received_message)
        network_logger.info(received_message)
        for peer in connected_peers:     
            try:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(received_message.encode('utf-8'))
                sock.close()
            except:
                continue

def gossip():
    for i in range(10):
        gossip_message = str(time.time()) + ":" + str(my_ip_address) + ":" + str(my_port) + ":" + "GOSSIP" + str(i+1) 
        MessageList.append(str(hashlib.sha256(gossip_message.encode()).hexdigest()))  
        for peer in connected_peers:
            try:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                peer_addr = peer.address.split(":")
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                sock.connect(ADDRESS)
                sock.send(gossip_message.encode('utf-8'))       
                sock.close() 
            except:
                print("Peer Down ", peer.address)
        time.sleep(5)

def execute_job():
    while True:
        x = job_queue.get()
        if x == 1:
            global sock
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            ADDRESS = (my_ip_address, my_port)
            sock.bind(ADDRESS)
            start_connection()
        elif x == 2:
            liveness_test()
        elif x == 3:
            gossip() 
        job_queue.task_done()

file = open("config.txt","r")
seeds_address_list = file.read()

temp = seeds_address_list.split("\n")

for addr in temp:
    if addr:
        addr = addr.split(":")
        addr = "172.31.57.82:" + str(addr[1])
        seeds_addresses.add(addr)
n = len(seeds_addresses)

register_with_k_seeds()     
for _ in range(num_threads):
        thread = threading.Thread(target = execute_job)
        thread.daemon = True
        thread.start()
for i in job_numbers:
    job_queue.put(i)
job_queue.join()
