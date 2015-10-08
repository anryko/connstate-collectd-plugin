#!/usr/bin/env python
# -*- coding: utf-8; -*-

import collectd
from collections import defaultdict, deque


# Global variables

NAME = 'connstate_info'
VERBOSE = True
PROTO = defaultdict(str, {
    'tcp': '/proc/net/tcp',
    'udp': '/proc/net/udp',
    'tcp6': '/proc/net/tcp6',
    'udp6': '/proc/net/udp6'
})
STATE = defaultdict(str, {
    '01': 'established',
    '02': 'syn_sent',
    '03': 'syn_recv',
    '04': 'fin_wait1',
    '05': 'fin_wait2',
    '06': 'time_wait',
    '07': 'close',
    '08': 'close_wait',
    '09': 'last_ack',
    '0A': 'listen',
    '0B': 'closing'
})


# Helper functions

def load_conns(fpath):
    """ Load protocol stats file
    """
    conns = deque(open(fpath, 'r'))
    conns.popleft()
    return conns


def get_states(conns_arr):
    """ Read state field from protocol stats array
    """
    states = deque()
    for conn in conns_arr:
        state = conn.split()[3]
        states.append(state)
    return states


def group_states(states_arr):
    """ Group states
    """
    global STATE
    grp_states = defaultdict(int, { k: 0 for k in STATE })
    for state in states_arr:
        grp_states[state] += 1
    return grp_states


def configure_callback(conf):
    """ Config data from collectd
    """
    log('verb', 'configure_callback Running')
    global NAME, VERBOSE
    for node in conf.children:
        if node.key == 'Name':
            NAME = node.values[0]
        elif node.key == 'Verbose':
            VERBOSE = node.values[0]
        else:
            log('warn', 'Unknown config key: {}'.format(node.key))


def read_callback():
    """ Prepare data for collectd
    """
    log('verb', 'read_callback Running')
    global PROTO, STATE
    for protocol, path in PROTO.iteritems():
        states = group_states(get_states(load_conns(path)))
        for state, count in states.iteritems():
            log('verb', 'Sending value for {}: {} {}'.format(protocol, STATE[state], count))
            value = collectd.Values(plugin=NAME)
            value.plugin_instance = protocol
            value.type = 'gauge'
            value.type_instance = STATE[state]
            value.values = [ count ]
            value.dispatch()


def log(t, message):
    """ Log messages to collect logger
    """
    if t == 'err':
        collectd.error('{}: {}'.format(NAME, message))
    elif t == 'warn':
        collectd.warning('{}: {}'.format(NAME, message))
    elif t == 'verb':
        if VERBOSE:
            collectd.info('{}: {}'.format(NAME, message))
    else:
        collectd.info('{}: {}'.format(NAME, message))


# Register to collectd
collectd.register_config(configure_callback)
collectd.warning('Initialising {}'.format(NAME))
collectd.register_read(read_callback)
