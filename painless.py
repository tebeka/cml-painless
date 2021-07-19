"""Task used in auto scaler spot crash

First time run, this task will spin a child process that will terminate the
machine. Second time it'll succeed.
"""

from argparse import ArgumentParser
from os import fork
from subprocess import run
from time import sleep

from clearml import Task

initial_state = 'initial'
remote_first_state = 'remote_first'
remote_second_state = 'remote_second'


def terminate_instance():
    cmd = ['sudo', 'shutdown', '-h', 'now']
    print(' '.join(cmd))
    return run(cmd).returncode == 0


if __name__ == '__main__':
    parser = ArgumentParser(description='painless task, used in spot testing')
    parser.add_argument('--queue', help='queue name', required=True)
    args = parser.parse_args()

    prop = 'painless_state'
    task = Task.init(project_name='clearml-test', task_name='painless')
    props = task.get_user_properties()

    state = props.get(prop, initial_state)
    if isinstance(state, dict):
        state = state['value']
    task.log.info('painless: state=%r', state)

    if state == initial_state:
        task.set_user_properties(**{prop: remote_first_state})
        task.execute_remotely(args.queue)  # This will exit the process
    elif state == remote_first_state:
        task.set_user_properties(**{prop: remote_second_state})
        if fork():
            num_minutes = 10
            task.log.info(
                'painless: parent waiting for %s minutes', num_minutes)
            for i in range(1, num_minutes * 60):
                print('iteration {}'.format(i))
                sleep(1)

            task.log.info('painless: still here after %s minutes', num_minutes)
            raise SystemExit('error: run full {} minutes'.format(num_minutes))
        else:  # child process
            task.log.info('painless: child terminating instance')
            if not terminate_instance():
                raise SystemExit('error: cannot terminate instance')
            raise SystemExit(0)
    elif state == remote_second_state:
        print('reborn!')
    else:
        raise SystemExit(f'error: unknown state - {state!r}')
