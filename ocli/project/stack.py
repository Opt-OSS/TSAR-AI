import os
import subprocess
from ocli.cli.state import Task
from ocli.project import _local_eodata_relative_path


def task_stack_snap(task: Task, dry_run, gpt_cache,cmd_dir,log):
    # TODO http://remote-sensing.eu/preprocessing-of-sentinel-1-sar-data-via-snappy-python-module/
    """ Run main-subordinate Stacking

    """
    snap_path = task.get_stack_path(full=True)
    log(f"Using ESA SNAP processing pipeline in  {snap_path}")
    os.makedirs(snap_path, exist_ok=True)
    cmd = os.path.join(cmd_dir, 'local-snap.sh')
    _eodata = task.config['eodata']
    _docker_mount = 'mnt'
    opts = [
        cmd,
        '--gpt-cache', gpt_cache,
        '--eodata', _eodata,
        '--snap_results', snap_path,
        '--swath', task.config['swath'],
        '--firstBurstIndex', task.config['firstBurstIndex'],
        '--lastBurstIndex', task.config['lastBurstIndex'],

        '--main', _local_eodata_relative_path(_eodata, task.config['main_path']),
        '--subordinate', _local_eodata_relative_path(_eodata, task.config['subordinate_path']),

    ]

    if dry_run:
        log("Command:")
        opts = opts + ['--dry-run']
    opts = opts + [' ']  # add space to the end for booleans
    # print(opts)
    # print(" ".join(opts))
    subprocess.run(opts)
