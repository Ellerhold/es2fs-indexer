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
        '/storage',
        is_type='mp',
        ev_types=fan.FAN_ALL_FID_EVENTS
    )
    fanot.start()

    cli = fan.FanotifyClient(fanot, path_pattern='/storage/*')
    poll = select.poll()
    poll.register(cli.sock.fileno(), select.POLLIN)
    try:
        while poll.poll():
            for i in cli.get_events():
                ev_types = fan.evt_to_str(i.ev_types)

                # if (ev_types != 'open') & (ev_types != 'close_nowrite'):
                print(
                    '%s: %s' % (ev_types, i.path)
                )
    except:
        print('STOP')

    cli.close()
    fanot.stop()