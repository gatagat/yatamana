from __future__ import (
        division, print_function, unicode_literals, absolute_import)
import os
import json
from contextlib import contextmanager
from .utils import which, run_cmd, makedirs, get_timestamp


@contextmanager
def cwd(dirname):
    '''Change working directory context manager.'''
    old_dir = os.getcwd()
    os.chdir(dirname)
    yield
    os.chdir(old_dir)


def stage_touch_all(meta, params):
    '''Touch all files below the target directory.'''
    find_path = which('find')
    target_dir = params['target'] % meta
    run_cmd([find_path, target_dir, '-exec', 'touch', '{}', ';'])


def stage_git(meta, params):
    '''Stage sources using git.'''
    git_path = which('git')
    target_dir = params['target'] % meta
    repo_url = params['repo_url']
    makedirs(target_dir)
    with cwd(target_dir):
        if os.path.isdir('.git'):
            run_cmd([git_path, 'pull'])
            run_cmd([git_path, 'submodule', 'update'])
        else:
            clone_opts = params.get('clone_opts', [])
            run_cmd([git_path, 'clone'] + clone_opts + [repo_url, '.'])
            run_cmd([git_path, 'submodule', 'init'])
            run_cmd([git_path, 'submodule', 'update'])


def stage_rsync(meta, params):
    '''Stage sources using rsync.'''
    rsync_path = which('rsync')
    target_dir = params['target'] % meta
    repo_url = params['repo_url']
    run_cmd([rsync_path, '-avuz', repo_url, target_dir])


def stage_symlink(meta, params):
    '''Stage a directory or file by symlinking.'''
    ln_path = which('ln')
    item = params['file']
    src_dir = item[0] % meta
    if len(item) == 1:
        dst_dir = src_dir
    else:
        dst_dir = item[1] % meta
    if os.path.lexists(dst_dir):
        actual_path = os.path.realpath(dst_dir)
        src_path = os.path.realpath(src_dir)
        if actual_path != src_path:
            raise RuntimeError(
                    'Path %s already exists pointing to %s instead of %s' % (
                        dst_dir, actual_path, src_path))
        return
    run_cmd([ln_path, '-s', src_dir, dst_dir])


def stage_copy(meta, params):
    '''Stage a file by copying.'''
    cp_path = which('cp')
    item = params['file']
    src_dir = item[0] % meta
    if len(item) == 1:
        dst_dir = src_dir
    else:
        dst_dir = item[1] % meta
    run_cmd([cp_path, src_dir, dst_dir])


def stage_do(meta, config):
    '''Stage according to the config.'''
    methods = {
        'touch': stage_touch_all,
        'git': stage_git,
        'rsync': stage_rsync,
        'copy': stage_copy,
        'symlink': stage_symlink}
    method = config['method']
    methods[method](meta, config.get('params'))


def stage_setup(config_filename, stage_dir):
    '''Setup staging.

    Helper for stage scripts that would take the paramters of
    this functions as command line arguments.
    '''
    config = json.load(open(config_filename))
    meta = config.get('meta', {})
    procedure = config.get('procedure', [])

    if stage_dir is None:
        stage_dir = meta['stage_dir']
    stage_dir = os.path.expandvars(stage_dir)
    stage_dir = stage_dir % {'timestamp': get_timestamp()}
    meta['stage_dir'] = stage_dir
    makedirs(stage_dir)

    return meta, procedure
