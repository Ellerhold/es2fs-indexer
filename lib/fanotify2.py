#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import select
import pyfanotify as fan


def foo(t):
    print(f'calling `foo` every {t} seconds')


if __name__ == '__main__':
    foo_timeout = 1
    fanot = fan.Fanotify(fn=foo, fn_args=(foo_timeout,), fn_timeout=foo_timeout, init_fid=True)
    fanot.mark(
        path='/storage',
        is_type='fs',
        ev_types=fan.FAN_ALL_FID_EVENTS
        #ev_types=fan.FAN_CREATE | fan.FAN_DELETE | fan.FAN_DELETE_SELF | fan.FAN_MOVE | fan.FAN_MOVE_SELF
    )
    fanot.start()

    cli = fan.FanotifyClient(fanot, path_pattern='/storage/*')
    poll = select.poll()
    poll.register(cli.sock.fileno(), select.POLLIN)
    try:
        while poll.poll():
            x = {}
            for i in cli.get_events():
                i.ev_types = fan.evt_to_str(i.ev_types)
                x.setdefault(i.path, []).append(i)
            if x:
                print(x)
    except:
        print('STOP')

    cli.close()
    fanot.stop()