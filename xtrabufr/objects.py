"""
xtrabufr.objects
~~~~~~~~~~~~~~~~~~
Objects to work with BUFR files
"""

from collections import MutableSequence as _MS
from . import definitions as _def


class Descriptors(_MS):
    """Descriptor(s) class

    Logic is based on that all descriptor(s) is(are) basically sequence.
    If code is a list of integers, then code is set to zero.
    """

    def __init__(self, code, masterTableVersionNumber='latest'):
        self.__dict__ = {'code': code, 'key': '', 'var_type': '',
                         'name': '', 'unit': '', 'scale': None,
                         'reference': None, 'width': None, 'crex_unit': None,
                         'crex_scale': None, 'crex_width': None,
                         '_list': []}
        seq = _def.get_sequence_def(masterTableVersionNumber)
        et = _def.get_element_table(masterTableVersionNumber)
        bt = _def.get_bufr_template_def()

        self.masterTableVersionNumber = masterTableVersionNumber

        if isinstance(code, list):
            self._list = [self.__class__(j) for j in code]
            self.code = 0

        if code in et.keys():
            self.key = et[code][0]
            self.var_type = et[code][1]
            self.name = et[code][2]
            self.unit = et[code][3]
            self.scale = et[code][4]
            self.reference = et[code][5]
            self.width = et[code][6]
            self.crex_unit = et[code][7]
            self.crex_scale = et[code][8]
            self.crex_width = et[code][9]
        elif code in seq.keys():
            if str(code) in bt.keys():
                self.key = bt[str(code)]
            self._list = [self.__class__(j) for j in seq[code]]

    def _check(self, v):
        if not isinstance(v, self.__class_):
            raise(TypeError(v))

    def _check_code(self, v):
        if not isinstance(v, int):
            raise(TypeError(v))

    def __repr__(self):
        if self.code == 0:
            s = ', '.join('{{{}}}'.format(i.code) for i in self._list)
            s = '[{}]'.format(s)
        elif self.F == 3:
            m = ', '.join(str(i.code) for i in self._list)
            s = '{{{:06d}}}:{{{}}}'.format(self.code, m)
        else:
            s = '{{{:06d}}} {} ({})'.format(self.code, self.name, self.unit)
        return(s)

    def __str__(self, show_desc=False, tab=0, leading=''):
        if self.code != 0:
            str_tab = ' ' * tab
            sc = '{:06d}'.format(self.code)
            s = '[{}]'.format(sc, self.key)
            if leading != '':
                s = ('\b' * len(leading)) + leading + s
            if show_desc and self.name != '':
                    s += ' {}'.format(self.name)
            else:
                if self.key != '':
                    s += ' {}'.format(self.key)
            if self.unit != '':
                s += ' ({})'.format(self.unit)
            if sc[0] == '1':
                s += ' ({}x{})'.format(int(sc[1:3]), int(sc[3:6]))
        else:
            str_tab = ''
            s = ''
            tab -= 4
        i = 0
        while i < len(self._list):
            sc2 = '{:06d}'.format(self._list[i].code)
            s += '\n' + self._list[i].__str__(show_desc, tab + 4)
            if sc2[0] == '1':
                sc3 = '{:06d}'.format(self._list[i + 1].code)
                if sc3[0:3] == '031':
                    i += 1
                    s += '\n' + self._list[i].__str__(show_desc, tab + 8,
                                                      '****')
                n = int(sc2[1:3])
                while n > 0:
                    i += 1
                    n -= 1
                    s += '\n' + self._list[i].__str__(show_desc, tab + 8,
                                                      '....')
            i += 1
        return(str_tab + s)

    def __len__(self):
        """List length"""
        return(len(self._list))

    def __iter__(self):
        return iter(self._list)

    def __getitem__(self, i):
        """Get a list item"""
        return(self._list[i])

    def __delitem__(self, i):
        """Delete an item"""
        del self._list[i]

    def __setitem__(self, i, val):
        self._check(val)
        self._list[i] = val

    @property
    def F(self):
        return(self.code // 100000)

    @property
    def X(self):
        return(self.code // 1000 % 100)

    @property
    def Y(self):
        return(self.code % 1000)

    @property
    def type(self):
        if self.code == 0:
            return('Sequence')
        if self.F == 0:
            return('Element')
        if self.F == 1 and self.Y == 0:
            return('DelayedReplicationDescriptor')
        if self.F == 1:
            return('FixedReplicationDescriptor')
        if self.F == 2:
            return('OperatorDescriptor')
        if self.F == 3:
            return('SequenceDescriptor')

        return(self.code % 1000)

    def insert(self, i, val):
        self._check(val)
        self._list.insert(i, val)

    def insert_code(self, i, code):
        self._check_code(code)
        d = self.__class__(code, self.masterTableVersionNumber)
        self._list.insert(i, d)

    def append_code(self, code):
        self._check_code(code)
        d = self.__class__(code, self.masterTableVersionNumber)
        self._list.append(d)

    def extend_code(self, code):
        if not isinstance(code, list):
            raise(TypeError(code))
        self._list.extend([self.__class__(j) for j in code])
