# ****************************************************************************
#
# Copyright (c) 2014-2024 Fraunhofer FKIE
# Author: Alexander Tiderko
# License: MIT
#
# ****************************************************************************

class JausAddress(object):
    '''
    JAUS address is defined by a triple {SubsystemID, NodeID, ComponentID}.
    Internal it is represented by a 4 byte integer value. You can create a new
    JAUS address with an integer value or use :meth:from_ids() to create it from
    id's.
    '''

    def __init__(self, value):
        object.__init__(self)
        self.value = 0
        if isinstance(value, int):
            self.value = value
        elif isinstance(value, JausAddress):
            self.value = value.value
        self._has_wildcards = None

    @property
    def zero(self):
        return self.value == 0

    @property
    def subsystem(self):
        return self.value >> 16

    @subsystem.setter
    def subsystem(self, value):
        self.value = self.value | (value << 16)

    @property
    def node(self):
        tempValue = self.value & 0x0000FF00
        return tempValue >> 8

    @node.setter
    def node(self, value):
        self.value = self.value | (value << 8)

    @property
    def component(self):
        return self.value & 0x000000FF

    @component.setter
    def component(self, value):
        self.value = self.value | value

    def has_wildcards(self):
        if self._has_wildcards is None:
            self._has_wildcards = self.node == 255 or self.component == 255 or self.subsystem == 65535
        return self._has_wildcards

    def __hash__(self):
        return self.value

    def __repr__(self):
        return 'JausAddress<%d.%d.%d-%d>' % (self.subsystem, self.node, self.component, self.value)

    @property
    def jaus_id(self):
        return '%d.%d.%d' % (self.subsystem, self.node, self.component)

    def __eq__(self, other):
        return self.value == other.value

    def match(self, other):
        '''
        If other contains wildcards checks own address for matches.
        '''
        if self == other:
            return True
        if other.subsystem != 65535 and self.subsystem != 65535 and self.subsystem != other.subsystem:
            return False
        if other.node != 255 and self.node != 255 and self.node != other.node:
            return False
        if other.component != 255 and self.component != 255 and self.component != other.component:
            return False
        return True

    @classmethod
    def from_ids(cls, subsystem=65535, node=255, component=255):
        '''
        :raise ValueError: if one of id's exceeds the maximum value. Does not check for negative values.
        '''
        if subsystem > 65535:
            raise ValueError("invalid subsystem ID: %d, max: 65535" % subsystem)
        if node > 255:
            raise ValueError("invalid node ID: %d, max: 255" % node)
        if component > 255:
            raise ValueError("invalid component ID: %d, max: 255" % component)
        result = JausAddress(0)
        result.subsystem = subsystem
        result.node = node
        result.component = component
        return result

    @classmethod
    def from_string(cls, strid):
        if strid.startswith('J'):
            # compatibility to JausToolSet configuration
            return JausAddress(int(strid[1:]))
        else:
            ids = strid.split('.')
            if len(ids) != 3:
                raise ValueError("invalid jaus address: %s, expected SubsystemID.NodeID.ComponentID" % strid)
            try:
                sid = int(ids[0])
                nid = int(ids[1])
                cid = int(ids[2])
                return cls.from_ids(sid, nid, cid)
            except ValueError as err:
                raise ValueError("invalid jaus address '%s': %s" % (strid, err))
