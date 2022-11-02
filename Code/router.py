import socket
import sys
import os
import json
import threading
import time

HOST = '127.0.0.1'  # localhost
BASE_TIMER = 3
MAX_METRIC = 16
NOT_SUPPORTED = -1
UPDATE_TIMER = BASE_TIMER * 6
ROUTE_TIMEOUT = UPDATE_TIMER * 2
DELETE_TIMEOUT = UPDATE_TIMER * 4


class Host:
    HOST = "127.0.0.1"

    def __init__(self, argv):
        self.id = int(argv[1])
        self.port = int(argv[2])
        self.ls = argv[3:]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.local_addr = (HOST, self.port)
        self.sock.bind(self.local_addr)
        """
            port & routing mapping
        """
        self.table = {}
        self.lock = threading.Lock()
        self.seq = []
        self.time = []
        self.failed_time = {}
        self.event = None
        self.timer = None
        self.start_node = False
        self.priority_route = {}
        self.ignore_nodes = set()
        # print("UDP Initialized")
        for adjacent_nodes in self.ls:
            # adjacent_info = {
            #     "dest_net": int(adjacent_nodes),
            #     "dist": 1,
            #     "next_hop": int(adjacent_nodes)
            # }
            self.seq.append(int(adjacent_nodes))
            self.time.append(time.time())
        # print("call waiting timer 46")
        self.create_waiting_timer()
        self.table[self.port] = {
            "dest_net": self.port,
            "dest_net_id": self.id,
            "dist": 0,
            "next_hop": self.port,
            "routing": [self.port]
        }
        # print("First Table Constructed")

    """
       to deliver message
    """

    def send_to_dest(self, data):
        ttl = data["ttl"]
        to_net_id = data["dest"]
        port_to_id = data["dest_id"]
        is_priority = data["is_priority"]
        if self.start_node:
            found_correspondent_port = False
            for port in self.table.keys():
                if self.table[port]["dest_net_id"] == to_net_id:
                    to_net_id = self.table[port]["dest_net"]
                    found_correspondent_port = True
                    break
            if not found_correspondent_port:
                to_net_id = -1
            if to_net_id in self.priority_route and self.priority_route[to_net_id][0]:
                is_priority = True
        if to_net_id in self.table and self.table[to_net_id]["dist"] == 0:
            print("destination " + str(port_to_id))
        elif ttl == 0:
            print("time exceed " + str(port_to_id))
        elif to_net_id in self.table and self.table[to_net_id]["dist"] < MAX_METRIC:
            next_hop = self.table[to_net_id]["next_hop"]
            if is_priority:
                next_hop = self.priority_route[to_net_id][1][1]
            dic_to_send = {
                "type": "routing_net",
                "dest": to_net_id,
                "ttl": ttl - 1,
                "dest_id": port_to_id,
                "is_priority": is_priority
            }
            dic_to_send_json = json.dumps(dic_to_send)
            self.sock.sendto(dic_to_send_json.encode(), (HOST, next_hop))
            if next_hop == to_net_id:
                print("Direct to " + str(port_to_id))
            else:
                print("Forward " + str(port_to_id))
        else:
            if self.start_node:
                print("No route to " + str(port_to_id))
            else:
                print("Dropped " + str(port_to_id))

    def send_priority_route(self, data):
        # print("received priority route")
        routing = data["routing"]
        reached_nodes = data["reached_nodes"]
        reached_nodes.append(self.port)
        if self.start_node:
            routing = self.id_to_port(routing)
        # print("routing: " + str(routing))
        if len(routing) == 0:
            return
        """
            has reached destination
        """
        if routing[-1] == self.port:
            print("priority routing change reached destination router")
            """
                change routing table
            """
            self.priority_route[self.port] = reached_nodes
            dic_to_send = {
                "type": "priority_config",
                "routing_reached": [],
                "routing_left": reached_nodes
            }
            self.handle_recvd_priority_config(dic_to_send)
            return
        if self.port in routing:
            routing.remove(self.port)
        idx = 0
        found = False
        while idx < len(routing):
            if routing[idx] not in self.table or self.table[routing[idx]]["dist"] == MAX_METRIC:
                idx += 1
                continue
            found = True
            break
        if not found:
            print("Can't reach destination route")
            return
        node_next = routing[idx]
        port_next = self.table[node_next]["next_hop"]
        dic_to_send = {
            "type": "priority_route",
            "routing": routing,
            "reached_nodes": reached_nodes
        }
        dic_to_send_json = json.dumps(dic_to_send)
        # print("priority next port " + str(port_next))
        self.sock.sendto(dic_to_send_json.encode(), (HOST, port_next))

    def send_ignore_node(self, data, addr=None):
        ignore_node = data["ignore_node"]
        if addr is None:
            found = False
            for node in self.table.keys():
                if self.table[node]["dest_net_id"] == ignore_node:
                    ignore_node = node
                    found = True
                    break
            if not found:
                print("No route to destination: " + str(ignore_node))
                return
        if ignore_node in self.ignore_nodes:
            return
        self.ignore_nodes.add(ignore_node)
        dic_to_send = {
            "type": "ignore_node",
            "ignore_node": ignore_node
        }
        dic_to_send_json = json.dumps(dic_to_send)
        for node in self.seq:
            if addr and node == addr[1]:
                continue
            self.sock.sendto(dic_to_send_json.encode(), (HOST, node))

    def send_to_neighbors(self):
        """
            dic_send_network contains:
                1. destination_network_id / destination_network_port
                2. distance to destination network
        :return:
        """
        net_port_send = {}
        """
            generate network info via assigned port/net for each port/net
        """
        self.lock.acquire()
        for nodes in self.table.keys():
            net_info = self.table[nodes]
            dest_net = net_info["dest_net"]
            dest_net_id = net_info["dest_net_id"]
            dist = net_info["dist"]
            routing = net_info["routing"]
            if dist >= MAX_METRIC:
                continue
            next_hop = net_info["next_hop"]
            if next_hop not in net_port_send.keys():
                net_port_send[next_hop] = []
            net_port_send[next_hop].append([dest_net, dest_net_id, dist, routing])
        dic_to_send = {
            "type": "routing_table",
            "origin_port": self.port,
            "network_infos": [],
        }
        """
            generate the message send via different port(Split Horizon & Poisoning Reverse)
            first update the failed node info
        """
        nodes_to_remove_set = set()
        for node in self.failed_time.keys():
            if time.time() - self.failed_time[node] > DELETE_TIMEOUT:
                nodes_to_remove_set.add(node)
        for node_to_remove in nodes_to_remove_set:
            self.failed_time.pop(node_to_remove)
        failed_nodes = self.failed_time.keys()
        failed_node_infos = []
        for failed_node in failed_nodes:
            failed_node_infos.append([self.table[failed_node]["dest_net"], self.table[failed_node]["dest_net_id"],
                                      self.table[failed_node]["dist"], self.table[failed_node]["routing"]])
        for next_hop in self.seq:
            to_send_nets = []
            """
                normal message send
            """
            for port in net_port_send.keys():
                if port == next_hop:
                    continue
                for net_info in net_port_send[port]:
                    to_send_nets.append(net_info)
            """
                failed note send
            """
            for _ in failed_node_infos:
                to_send_nets.append(_)
            """
                generate sending info
            """
            dic_to_send["network_infos"] = to_send_nets
            # print(dic_to_send)
            dic_to_send_json = json.dumps(dic_to_send)
            self.sock.sendto(dic_to_send_json.encode(), (HOST, next_hop))
        self.lock.release()

    def create_sending_timer(self):
        t = threading.Timer(UPDATE_TIMER, self.start_sending)
        t.setDaemon(True)
        t.start()

    def start_sending(self):
        self.send_to_neighbors()
        self.create_sending_timer()

    def start_listening(self):
        thread_host_listening = threading.Thread(target=self.listening, name="listening thread")
        thread_host_listening.setDaemon(True)
        thread_host_listening.start()

    def listening(self):
        while True:
            response, addr_response = self.sock.recvfrom(2048)
            """
                first receive or recover
            """
            if self.timer is None:
                self.timer_update(addr_response)
            else:
                self.timer_update(addr_response)
            data = response.decode()
            data = json.loads(data)
            if data["type"] == "routing_table":
                self.handle_recvd_table(data)
            elif data["type"] == "routing_net":
                self.send_to_dest(data)
            elif data["type"] == "priority_route":
                self.send_priority_route(data)
            elif data["type"] == "priority_config":
                self.handle_recvd_priority_config(data)
            elif data["type"] == "ignore_node":
                self.send_ignore_node(data, addr_response)

    def handle_recvd_priority_config(self, data):
        routing_reached = data["routing_reached"]
        routing_left = data["routing_left"]
        if routing_left[-1] != self.port:
            print("fault in routing")
        routing_reached.insert(0, self.port)
        self.priority_route[routing_reached[-1]] = [False, routing_reached]
        """
            record ud
        """
        print("Priority config: " + str(self.priority_route))
        """
            has reached start route
        """
        if len(routing_left) == 1:
            self.priority_route[routing_reached[-1]][0] = True
            self.data_to_write(self.generate_key(routing_reached[-1]), self.port_to_id(routing_reached))
            return
        routing_left = routing_left[:-1]
        dic_to_send = {
            "type": "priority_config",
            "routing_reached": routing_reached,
            "routing_left": routing_left
        }
        next_hop = routing_left[-1]
        dic_to_send_json = json.dumps(dic_to_send)
        self.sock.sendto(dic_to_send_json.encode(), (HOST, next_hop))

    def handle_recvd_table(self, data):
        self.lock.acquire()
        origin_port = int(data["origin_port"])
        new_networks = data["network_infos"]
        for network in new_networks:
            dest_net = int(network[0])
            dest_net_id = int(network[1])
            dest_dst = int(network[2])
            dest_rout = network[3]
            """
                check whether the port has failed
            """
            if dest_dst + 1 >= MAX_METRIC:
                """
                    set net via failed node as unreachable
                """
                if dest_net in self.table.keys() and self.table[dest_net]["dist"] < MAX_METRIC and \
                        self.table[dest_net]["next_hop"] == origin_port:
                    self.failed_time[dest_net] = time.time()
                elif dest_net not in self.table.keys():
                    self.failed_time[dest_net] = time.time()
            """
                update from unreachable to reachable
            """
            if dest_dst + 1 < MAX_METRIC:
                if dest_net in self.table.keys() and \
                        self.table[dest_net]["dist"] == MAX_METRIC and dest_net in self.failed_time.keys():
                    self.failed_time.pop(dest_net)
            """
                update routing table
            """
            """
                check whether the routing contains ignored node
            """
            ignored = False
            for ignore_node in self.ignore_nodes:
                if ignore_node in dest_rout:
                    ignored = True
                    break
            if ignored and self.port not in self.ignore_nodes and dest_dst + 1 <= MAX_METRIC:
                continue
            update = False
            if dest_net in self.table:
                if self.table[dest_net]["next_hop"] == origin_port:
                    self.table[dest_net]["dist"] = MAX_METRIC if dest_dst + 1 >= MAX_METRIC else dest_dst + 1
                    self.table[dest_net]["dest_net_id"] = dest_net_id
                    update = True if self.table[dest_net]["routing"] == [self.port].extend(dest_rout) else False
                    self.table[dest_net]["routing"] = [self.port]
                    self.table[dest_net]["routing"].extend(dest_rout)
                elif self.table[dest_net]["dist"] > dest_dst + 1:
                    self.table[dest_net]["dist"] = MAX_METRIC if dest_dst + 1 >= MAX_METRIC else dest_dst + 1
                    self.table[dest_net]["dest_net_id"] = dest_net_id
                    self.table[dest_net]["next_hop"] = origin_port
                    self.table[dest_net]["routing"] = [self.port]
                    self.table[dest_net]["routing"].extend(dest_rout)
                    update = True
            else:
                self.table[dest_net] = {
                    "dest_net": dest_net,
                    "dest_net_id": dest_net_id,
                    "dist": MAX_METRIC if dest_dst + 1 >= MAX_METRIC else dest_dst + 1,
                    "next_hop": origin_port,
                    "routing": [self.port]
                }
                self.table[dest_net]["routing"].extend(dest_rout)
                update = True
            if self.table[dest_net]["dist"] == MAX_METRIC:
                self.table[dest_net]["next_hop"] = -1
                self.table[dest_net]["routing"] = [-1]
                update = True
                # self.data_to_write(self.generate_key(self.table[dest_net]["dest_net_id"]),
                #                    self.port_to_id(self.table[dest_net]["routing"]))
            """
                record ud
            """
            if update:
                self.data_to_write(self.generate_key(self.table[dest_net]["dest_net_id"]),
                                   self.port_to_id(self.table[dest_net]["routing"]))
        self.lock.release()
        # print(self.table)

    def create_waiting_timer(self):
        if len(self.seq) == 0:
            self.timer = None
            return
        self.timer = threading.Timer(ROUTE_TIMEOUT - (time.time() - self.time[0]), self.some_node_failed)
        self.timer.setDaemon(True)
        self.timer.start()

    def some_node_failed(self):
        self.lock.acquire()
        node_failed = self.seq[0]
        """
            set as unreachable node
        """
        if node_failed not in self.table:
            """
                init dic first
            """
            self.table[node_failed] = {}
            self.table[node_failed]["dest_net"] = node_failed
            self.table[node_failed]["dest_net_id"] = -1
        self.table[node_failed]["dist"] = MAX_METRIC
        self.table[node_failed]["next_hop"] = -1
        self.table[node_failed]["routing"] = [-1]
        self.failed_time[node_failed] = time.time()
        """
            record ud
        """
        self.data_to_write(self.generate_key(self.table[node_failed]["dest_net_id"]),
                           self.port_to_id(self.table[node_failed]["routing"]))
        print("node has failed: " + str(self.seq[0]))
        """
            set the net pass through the failed node as unreachable
            and update the correspondent routing table
        """
        for net in self.table.keys():
            if self.table[net]["next_hop"] == node_failed and net != node_failed:
                self.table[net]["dist"] = MAX_METRIC
                self.table[net]["next_hop"] = -1
                self.table[net]["routing"] = [-1]
                self.failed_time[net] = time.time()
                """
                    record ud
                """
                self.data_to_write(self.generate_key(self.table[net]["dest_net_id"]),
                                   self.port_to_id(self.table[net]["routing"]))
        self.seq.pop(0)
        self.time.pop(0)
        self.lock.release()
        """
            Use the next node as timer in seq
        """
        self.timer_update()

    def timer_update(self, addr=None):
        """
        :param addr: address
        :return: None
        if the timer is on the response node
        """
        """
            node failed
        """
        if addr is None:
            # print("call waiting timer 231")
            self.create_waiting_timer()
            return
        """
            first receive or recover
        """
        if self.timer is None:
            self.seq.append(addr[1])
            self.time.append(time.time())
            # print("call waiting timer 237")
            self.create_waiting_timer()
            return
        """
            add adjacent nodes
        """
        if self.seq[0] == addr[1]:
            self.timer.cancel()
            self.seq.pop(0)
            self.time.pop(0)
            """
                Add first in case of the list is empty
            """
            self.seq.append(addr[1])
            self.time.append(time.time())
            # start new timer
            # print("call waiting timer 253")
            self.create_waiting_timer()
            """
                update timer for listening
            """
        elif addr[1] in self.seq:
            idx = self.seq.index(addr[1])
            self.seq.pop(idx)
            self.time.pop(idx)
            self.seq.append(addr[1])
            self.time.append(time.time())
        else:
            self.seq.append(addr[1])
            self.time.append(time.time())

    def print_routing_table(self):
        print("Destination       route")
        if len(self.table) == 1:
            print("No available routing")
        for node in self.table.keys():
            if node == self.port or (node in self.priority_route and self.priority_route[node][0] is True):
                continue
            print(str(self.table[node]["dest_net_id"]), end="                 ")
            port_to_id = self.port_to_id(self.table[node]["routing"])
            print(*port_to_id, sep=", ")
        for node in self.priority_route.keys():
            if self.priority_route[node][0]:
                print(str(self.table[self.priority_route[node][1][-1]]["dest_net_id"]), end="                 ")
                port_to_id = self.port_to_id(self.priority_route[node][1])
                print(*port_to_id, sep=", ")

    def print_adjacent_nodes(self):
        print("Adjacent nodes")
        if len(self.seq) == 0:
            print("No adjacent nodes")
        else:
            port_to_id = self.port_to_id(self.seq)
            print(*port_to_id, sep=", ")

    def print_statistic_info(self):
        with open("router_update_{}.json".format(self.id), "r", encoding="utf-8") as f:
            data = json.load(f)
            f.close()
            print("update time and route id              " + "updated route")
            for key in data.keys():
                value = data[key]
                print(key, end="    ")
                print(*value, sep=", ")

    def port_to_id(self, ports):
        if ports[0] == -1:
            return ["No route to desired destination"]
        return [self.table[port]["dest_net_id"] for port in ports]

    def id_to_port(self, ids):
        port_list = []
        to_int = set()
        for idx in ids:
            to_int.add(int(idx))
        for port in self.table.keys():
            if self.table[port]["dest_net_id"] in to_int:
                port_list.append(port)
        return port_list

    def data_to_write(self, key, value):
        with open("router_update_{}.json".format(self.id), "r+", encoding="utf-8") as f:
            file = f.read()
            json_file = {}
            if len(file) > 0:
                json_file = json.loads(file)
        with open("router_update_{}.json".format(self.id), "w", encoding="utf-8") as f:
            json_file.update({key: value})
            json.dump(json_file, f)
            f.close()

    def generate_key(self, dest):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " Router " + str(dest) + "          "

    def processing_input_commands(self):
        if not os.path.exists("router_update_{}.json".format(self.id)):
            with open("router_update_{}.json".format(self.id), "w", encoding="utf-8"):
                print("File created: " + "router_update_{}.json".format(self.id))
        while True:
            line = input("enter command here: ")
            commands = line.split(" ")
            if commands[0] == "RT":
                self.print_routing_table()
            elif commands[0] == "N":
                self.print_adjacent_nodes()
            elif commands[0] == "D":
                to_net_id = int(commands[1])
                time_to_live = MAX_METRIC if len(commands) < 3 else int(commands[2])
                self.start_node = True
                self.send_to_dest({"type": "routing_net", "dest": to_net_id, "ttl": time_to_live, "dest_id": to_net_id,
                                   "is_priority": False})
                self.start_node = False
            elif commands[0] == "P":
                routing = commands[2:]
                self.start_node = True
                self.send_priority_route({"type": "priority_route", "routing": routing, "reached_nodes": []})
                self.start_node = False
            elif commands[0] == "S":
                self.print_statistic_info()
            elif commands[0] == "R":
                refused_port = int(commands[1])
                self.send_ignore_node({"type": "ignore_node", "ignore_node": refused_port})


cur_host = Host(sys.argv)

"""
   start listening 
"""

cur_host.start_listening()

"""
   running the UDP send per 30 sec
"""

cur_host.start_sending()

cur_host.processing_input_commands()
