###################
# Course: CSE 138
# Quarter: Winter 2023
# Assignment: #3
# Author: Amin Karbas <mkarbasf@ucsc.edu>
###################

# Preferably, these should be run when nodes are partitioned, too.

import sys
import time
import unittest

import requests  # pip install requests


# Setup:


def usage():
    print(
        f'Usage: {sys.argv[0]} local_port1:ip1:port1 local_port2:ip2:port2 [local_port3:ip3:port3...]')
    sys.exit(1)


def check_arg_count():
    if len(sys.argv) < 3:
        usage()


def parse_args():
    check_arg_count()
    local_ports = []
    view = []
    for arg in sys.argv[1:]:
        try:
            col1_idx = arg.find(':')
            local_ports.append(int(arg[:col1_idx]))
            view.append(arg[col1_idx+1:])
        except:
            usage()
    return local_ports, view


ports, view_addresses = parse_args()
hosts = ['localhost'] * len(ports)
keys = ['key1', 'key2', 'key3']
vals = ['Value 1', 'val2', 'third_value']
causal_metadata_key = 'causal-metadata'


# Requests:


def get(url, body={}):
    return requests.get(url, json=body)


def put(url, body={}):
    return requests.put(url, json=body)


def delete(url, body={}):
    return requests.delete(url, json=body)


# URLs:


def make_base_url(port, host='localhost', protocol='http'):
    return f'{protocol}://{host}:{port}'


def kvs_view_admin_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/admin/view'


def kvs_data_key_url(key, port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data/{key}'


def kvs_data_url(port, host='localhost'):
    return f'{make_base_url(port, host)}/kvs/data'

def partition_url(port, host='localhost', protocal='http'):
    return f'{make_base_url(port, host)}/kvs/admin/partition'


# Bodies:


def nodes_list(ports, hosts=None):
    if hosts is None:
        hosts = ['localhost'] * len(ports)
    return [f'{h}:{p}' for h, p in zip(hosts, ports)]


def put_view_body(addresses):
    return {'view': addresses}

def put_partition_body(partition):
    return { 'partition': partition }

def causal_metadata_body(cm={}):
    return {causal_metadata_key: cm}


def causal_metadata_from_body(body):
    return body[causal_metadata_key]


def put_val_body(val, cm=None):
    body = causal_metadata_body(cm)
    body['val'] = val
    #print(body)
    return body


class TestAssignment1(unittest.TestCase):
    def setUp(self):
        # Uninitialize all nodes:
        for h, p in zip(hosts, ports):
            delete(partition_url(p, h))
            delete(kvs_view_admin_url(p, h))

    def test_uninitialized_get_key(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p):
                res = get(kvs_data_key_url('some_key', p, h))
                self.assertEqual(res.status_code, 418,
                                 msg='Bad status code (not a teapot!)')
                body = res.json()
                self.assertIn('error', body,
                              msg='Key not found in json response')
                self.assertEqual(body['error'], 'uninitialized',
                                 msg='Bad error message')

    def test_uninitialized_get_view(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p):
                res = get(kvs_view_admin_url(p, h))
                self.assertEqual(res.status_code, 200, msg='Bad status code')
                body = res.json()
                self.assertIn('view', body,
                              msg='Key not found in json response')
                self.assertEqual(body['view'], [], msg='Bad view')

    def test_put_get_view(self):
        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p, verb='put'):
                res = put(kvs_view_admin_url(p, h),
                          put_view_body(view_addresses))
                self.assertEqual(res.status_code, 200, msg='Bad status code')

        for h, p in zip(hosts, ports):
            with self.subTest(host=h, port=p, verb='get'):
                res = get(kvs_view_admin_url(p, h))
                self.assertEqual(res.status_code, 200, msg='Bad status code')
                body = res.json()
                self.assertIn('view', body,
                              msg='Key not found in json response')
                self.assertEqual(body['view'], view_addresses,
                                 msg='Bad view')

    def test_spec_ex2(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        time.sleep(1)

        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[1], cm1))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[0], cm1))
        self.assertEqual(res.status_code, 201, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm1 = causal_metadata_from_body(body)

        res = get(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  causal_metadata_body())
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)
        self.assertIn('val', body, msg='Key not found in json response')
        self.assertEqual(body['val'], vals[0], 'Bad value')

        res = get(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  causal_metadata_body(cm2))
        self.assertIn(res.status_code, {200, 500}, msg='Bad status code')
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm2 = causal_metadata_from_body(body)

        if res.status_code == 200:
            self.assertIn('val', body, msg='Key not found in json response')
            self.assertEqual(body['val'], vals[1], 'Bad value')
            return

        # 500
        self.assertIn('error', body, msg='Key not found in json response')
        self.assertEqual(body['error'], 'timed out while waiting for depended updates',
                         msg='Bad error message')

    def test_tie_breaking(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[1]))
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        time.sleep(10)

        res0 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                   causal_metadata_body())
        self.assertEqual(res0.status_code, 200, msg='Bad status code')
        body = res0.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val0 = body['val']
        self.assertIn(val0, {vals[0], vals[1]}, 'Bad value')

        res1 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                   causal_metadata_body())
        self.assertEqual(res0.status_code, 200, msg='Bad status code')
        body = res1.json()
        self.assertIn('val', body, msg='Key not found in json response')
        val1 = body['val']
        self.assertIn(val1, {vals[0], vals[1]}, 'Bad value')

        self.assertEqual(val0, val1, 'Bad tie-breaking')

    def test_key_list(self):
        res = put(kvs_view_admin_url(ports[0], hosts[0]),
                  put_view_body(view_addresses))
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        res = put(kvs_data_key_url(keys[0], ports[0], hosts[0]),
                  put_val_body(vals[0]))
        body = res.json()
        
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertEqual(res.status_code, 201, msg='Bad status code')

        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[1], cm))
        body = res.json()
        self.assertIn(causal_metadata_key, body,
                      msg='Key not found in json response')
        cm = causal_metadata_from_body(body)
        self.assertIn(res.status_code, {200, 201}, msg='Bad status code')

        time.sleep(10)

        res = get(kvs_data_url(ports[0], hosts[0]), causal_metadata_body(cm))
        self.assertEqual(res.status_code, 200, 'Bad status code')
        body = res.json()
        self.assertIn('count', body,
                      msg='Key not found in json response')
        self.assertEqual(body['count'], 2, 'Bad count')
        self.assertIn('keys', body,
                      msg='Key not found in json response')
        self.assertEqual(body['keys'], keys[:2], 'Bad keys')

    def test_eventual_consistency_partition_1(self):
        # initialize the view
        new_view_addresses = view_addresses;
        res = put(
                kvs_view_admin_url(ports[0], hosts[0]), 
                put_view_body(new_view_addresses)
        );
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        
        
        # ----------------- Start Create Partitions --------------------

        # create a partition for replica 1
        res = put(
            partition_url(ports[0], hosts[0]), 
            put_partition_body([view_addresses[0]])
        )

        # create a partition for replica 2
        res = put(
            partition_url(ports[1], hosts[1]), 
            put_partition_body([view_addresses[1]])
        )

        # ----------------- End Create Partitions --------------------
        
        # ----------------- Start Put Data --------------------

        # put val in replica 1
        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[0]))
        cm1 = causal_metadata_from_body(res.json());

        # put val in replica 2
        res = put(kvs_data_key_url(keys[1], ports[1], hosts[1]),
                  put_val_body(vals[1]))

        cm2 = causal_metadata_from_body(res.json());
        # ----------------- End Put Data --------------------
        
        
        # ----------------- Start Delete Partition --------------------
        
        delete(partition_url(ports[0], hosts[0]));
        delete(partition_url(ports[1], hosts[1]));

        # ----------------- End Delete Partition --------------------

        time.sleep(5)
        
        # ----------------- Test Consistency --------------------

        res0 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, vals[0], 'Bad value')

        res1 = get(kvs_data_key_url(keys[1], ports[0], hosts[0]), causal_metadata_body())
        body1 = res1.json();
        self.assertIn('val', body1, 'No value')
        val1 = body1['val']
        self.assertEqual(val1, vals[1], 'Bad value')


        res0 = get(kvs_data_key_url(keys[0], ports[1], hosts[1]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, vals[0], 'Bad value')

        res1 = get(kvs_data_key_url(keys[1], ports[1], hosts[1]), causal_metadata_body())
        body1 = res1.json();
        self.assertIn('val', body1, 'No value')
        val1 = body1['val']
        self.assertEqual(val1, vals[1], 'Bad value')


        res0 = get(kvs_data_key_url(keys[0], ports[2], hosts[2]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, vals[0], 'Bad value')

        res1 = get(kvs_data_key_url(keys[1], ports[2], hosts[2]), causal_metadata_body())
        body1 = res1.json();
        self.assertIn('val', body1, 'No value')
        val1 = body1['val']
        self.assertEqual(val1, vals[1], 'Bad value')
        
    def test_eventual_consistency_partition_2(self):
        # initialize the view
        new_view_addresses = view_addresses;
        res = put(
                kvs_view_admin_url(ports[0], hosts[0]), 
                put_view_body(new_view_addresses)
        );
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        

        # ----------------- Start Create Partitions --------------------

        # create a partition for replica 1
        res = put(
            partition_url(ports[0], hosts[0]), 
            put_partition_body([view_addresses[0]])
        )

        # create a partition for replica 2
        res = put(
            partition_url(ports[1], hosts[1]), 
            put_partition_body([view_addresses[1]])
        )

        # create a partition for replica 2
        res = put(
            partition_url(ports[2], hosts[2]), 
            put_partition_body([view_addresses[2]])
        )

        # ----------------- End Create Partitions --------------------

        # ----------------- Start Put Data --------------------

        # put val in replica 1
        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[0]))
        cm1 = causal_metadata_from_body(res.json());

        # put val in replica 2
        res = put(kvs_data_key_url(keys[0], ports[1], hosts[1]),
                  put_val_body(vals[1]))
        cm2 = causal_metadata_from_body(res.json());

        # ----------------- End Put Data --------------------


        # ----------------- Start Delete Partition --------------------
        
        delete(partition_url(ports[0], hosts[0]));
        delete(partition_url(ports[1], hosts[1]));
        delete(partition_url(ports[2], hosts[2]));

        # ----------------- End Delete Partition --------------------

        time.sleep(5)
        
        # ----------------- Test Consistency --------------------

        res0 = get(kvs_data_key_url(keys[0], ports[0], hosts[0]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']

        res0 = get(kvs_data_key_url(keys[0], ports[1], hosts[1]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val1 = body0['val']
        self.assertEqual(val0, val1, 'Bad value')

        res1 = get(kvs_data_key_url(keys[0], ports[2], hosts[2]), causal_metadata_body())
        body1 = res1.json();
        self.assertIn('val', body1, 'No value')
        val2 = body1['val']
        self.assertEqual(val1, val2, 'Bad value')

    def test_causal_consistency(self):
        # initialize the view
        new_view_addresses = view_addresses;
        res = put(
                kvs_view_admin_url(ports[0], hosts[0]), 
                put_view_body(new_view_addresses)
        );
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        

        # ----------------- Start Create Partitions --------------------

        # create a partition for replica 1
        res = put(
            partition_url(ports[0], hosts[0]), 
            put_partition_body([view_addresses[0]])
        )

        # create a partition for replica 2
        res = put(
            partition_url(ports[1], hosts[1]), 
            put_partition_body([view_addresses[1]])
        )

        # create a partition for replica 3/4
        res = put(
            partition_url(ports[2], hosts[2]), 
            put_partition_body([view_addresses[2], view_addresses[3]])
        )

        res = put(
            partition_url(ports[3], hosts[3]), 
            put_partition_body([view_addresses[2], view_addresses[3]])
        )

        # ----------------- End Create Partitions --------------------

        # ----------------- Start Put Data --------------------

        # put val in replica 1
        res = put(kvs_data_key_url("x", ports[0], hosts[0]),
                  put_val_body("1"))
        cm1 = causal_metadata_from_body(res.json());

        # put val in replica 2
        res = put(kvs_data_key_url("x", ports[1], hosts[1]),
                  put_val_body("2", cm1))

        res = put(kvs_data_key_url("y", ports[1], hosts[1]),
                  put_val_body("1"))
        cm2 = causal_metadata_from_body(res.json());

        res = put(kvs_data_key_url("y", ports[0], hosts[0]),
                  put_val_body("2", cm2))

        # ----------------- End Put Data --------------------


        # ----------------- Start Delete Partition --------------------
        
        delete(partition_url(ports[0], hosts[0]));
        delete(partition_url(ports[1], hosts[1]));
        delete(partition_url(ports[2], hosts[2]));
        delete(partition_url(ports[3], hosts[3]));

        # ----------------- End Delete Partition --------------------

        time.sleep(5)

        # ----------------- Test Consistency --------------------

        res0 = get(kvs_data_key_url("x", ports[0], hosts[0]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");

        res0 = get(kvs_data_key_url("y", ports[0], hosts[0]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");


        res0 = get(kvs_data_key_url("x", ports[1], hosts[1]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");

        res0 = get(kvs_data_key_url("y", ports[1], hosts[1]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");


        res0 = get(kvs_data_key_url("x", ports[2], hosts[2]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");

        res0 = get(kvs_data_key_url("y", ports[2], hosts[2]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");
        
    def test_non_healing_network_partition(self):
        # this tests whether it hangs if there's a down node in the view

        # initialize the view
        new_view_addresses = [ view_addresses[0], "10.10.0.6:9999" ];
        res = put(
                kvs_view_admin_url(ports[0], hosts[0]), 
                put_view_body(new_view_addresses)
        );
        self.assertEqual(res.status_code, 200, msg='Bad status code')

        # put value to replica
        res = put(kvs_data_key_url("x", ports[0], hosts[0]),
                  put_val_body("1"))
        cm1 = causal_metadata_from_body(res.json());

        # get value from replica
        res0 = get(kvs_data_key_url("x", ports[0], hosts[0]), causal_metadata_body(cm1))
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "1");
        
    def test_fuck_you_alice(self):
        # initialize the view
        new_view_addresses = view_addresses;
        res = put(
                kvs_view_admin_url(ports[0], hosts[0]), 
                put_view_body(new_view_addresses)
        );
        self.assertEqual(res.status_code, 200, msg='Bad status code')
        

        # ----------------- Start Create Partitions --------------------

        # create a partition for replica 1
        res = put(
            partition_url(ports[0], hosts[0]), 
            put_partition_body([view_addresses[0]])
        )

        # create a partition for replica 2
        res = put(
            partition_url(ports[1], hosts[1]), 
            put_partition_body([view_addresses[1]])
        )

        # create a partition for replica 3/4
        res = put(
            partition_url(ports[2], hosts[2]), 
            put_partition_body([view_addresses[2], view_addresses[3]])
        )

        res = put(
            partition_url(ports[3], hosts[3]), 
            put_partition_body([view_addresses[2], view_addresses[3]])
        )

        # ----------------- End Create Partitions --------------------

        # ----------------- Start Put Data --------------------

        # put val in replica 1
        res = put(kvs_data_key_url("x", ports[0], hosts[0]),
                  put_val_body("1"))
        cm1 = causal_metadata_from_body(res.json());

        res0 = get(kvs_data_key_url("x", ports[0], hosts[0]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "1");

        cm2 = causal_metadata_from_body(body0);

        # put val in replica 2
        res = put(
                kvs_data_key_url("y", ports[1], hosts[1]),
                put_val_body("2", cm2)
        )

        res0 = get(kvs_data_key_url("y", ports[1], hosts[1]), causal_metadata_body())
        body0 = res0.json();
        self.assertIn('val', body0, 'No value')
        val0 = body0['val']
        self.assertEqual(val0, "2");

        cm2 = causal_metadata_from_body(body0);

        res0 = get(kvs_data_key_url("x", ports[1], hosts[1]), causal_metadata_body(cm2))
        self.assertEqual(res0.status_code, 500, "Should've stalled");

        # ----------------- End Put Data --------------------


        # ----------------- Start Delete Partition --------------------
        
        delete(partition_url(ports[0], hosts[0]));
        delete(partition_url(ports[1], hosts[1]));
        delete(partition_url(ports[2], hosts[2]));
        delete(partition_url(ports[3], hosts[3]));

        # ----------------- End Delete Partition --------------------

        time.sleep(5);

        res0 = get(kvs_data_key_url("x", ports[1], hosts[1]), causal_metadata_body(cm2))
        body0 = res0.json();
        self.assertIn('val', body0, "No value");
        val1 = body0['val'];
        self.assertEqual(val1, "1", "error not in kvs");

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
