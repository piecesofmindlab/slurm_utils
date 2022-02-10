# Functions to send jobs to slurm cluster via python

import os
import re
import sys
import uuid
import time
import socket
import tempfile
import textwrap
import subprocess
import pickle

# For portability of this file only to other modules.
try:
    from .options import config
    default_logdir = config.get('cluster', 'logdir')
except ImportError:
    raise Exception('No slurm log directory set in options.cfg file!\nPlease edit your options.cfg file in the directory with the vm_tools source code!')
"""  """
def get_uuid():
    return str(uuid.uuid4()).replace('-', '')

def run_local(script, check=False, capture_output=False):
	tf = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.py')
	tf.write(script)
	tf.close()
	# For backward compatibility with python 3.6
	major, minor, _ = sys.version.split(' ')[0].split('.')
	if (int(major), int(minor)) < (3, 7):
		kw = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	else:
		kw = dict(capture_output=capture_output)
	proc = subprocess.run(['python3', tf.name], check=True, **kw)
	if capture_output:
		so = proc.stdout.decode()
		se = proc.stderr.decode()
		print(so)
		if not se == '':
			print(se)
	# cleanup
	os.unlink(tf.name)
	return proc

def run_script(script, 
               cmd='python3',
               logdir=default_logdir, 
               slurm_out='slurm_pyscript_node_%N_job_%j.out',
               slurm_err=None,
               job_name=None,
               dep=None,
               mem=30,
               mem_per_cpu=3500,
               ncpus=3,
               time_limit=None,
               partition='regular',
               account=None,
               remote_host=None,
               singularity_container=None,
               singularity_drive_mount=None):
    """Run a python script on the cluster.
    
    Parameters
    ----------
    script : string
        Either a full script (as a string, probably in triple quoters) or a path to a script file
    cmd : str, optional
        Description
    logdir : string
        Directory in which to store slurm logs
    slurm_out : string
        File name for slurm log file. For slurm outputs, %j is job id; %N is node name
    slurm_err : string
        File name for separate error file (if desired - if slurm_err is None, errors are 
        written to the `slurm_out` file)
    job_name : string
        Name that will appear in the slurm queue
    dep : string | None
        String job numbers for dependencies, e.g. '78823, 78824, 78825' (for 
        3 slurm job dependencies with those job id numbers). default=None
    mem : scalar
        Memory usage in gigabytes 
    mem_per_cpu : int, optional
        Minimum memory required per allocated CPU. Default units are megabytes
    ncpus : scalar {1 OR 3}
        Number of CPUs requested. Glab convention is to request 1 cpu per ~8GB of memory
    time_limit : int, string
        Time limit on total run time of the job.  Lower times have higher priority, 
        but when the time limit is reached job may be killed.  Single int means in minutes.
        String formats   include   "minutes",   "minutes:seconds", "hours:minutes:seconds",  
        "days-hours", "days-hours:minutes" and "days-hours:minutes:seconds".
    partition : string
        Either 'regular' or 'big' (regular = 32GB memory max, big = 64GB memory max)
    account : None, optional
        Description
    remote_host : None, optional
        Description
    singularity_container : None, optional
        Description
    singularity_drive_mount : None, optional
        Description
    
    Returns
    -------
    TYPE
        Description
    
    Raises
    ------
    Exception
        Description
    """
    if os.path.exists(script):
        with open(script, 'rb') as fid:
            script=fid.read()
    # Parse sbatch parameters
    if slurm_err is True:
        slurm_err = 'Error_'+slurm_out
    if job_name is None:
        job_name = 'vmt_autogen_'+time.strftime('%Y_%m_%d_%H%M', time.localtime())
    logfile = os.path.join(logdir, slurm_out)
    # Set up sbatch script
    # (TODO: currently written to disk. See if we can avoid this.)
    header = '#!/usr/bin/env python3\n#SBATCH\n\n'
    if slurm_err is None:
        error_handling = ""
    else:
        #tmp_err = os.path.join('/tmp/', slurm_err)
        errfile = os.path.join(logdir, slurm_err)
        error_handling = textwrap.dedent("""
        # Cleanup error files
        import socket, os
        ef = '{slurm_err}'.replace('%j', os.getenv('SLURM_JOB_ID'))
        ef = ef.replace('%N', socket.gethostname())
        ef = os.path.expanduser(ef)
        with open(ef, 'r') as fid:
            output=fid.read()
        if len(output)>0:
            print('Warnings detected!')
            print(output)
            os.unlink(ef)
        else:
            print('Cleanup -> removing ' + ef)
            os.unlink(ef)
        """).format(slurm_err=errfile) # write error file locally
        
    # Create full script     
    python_script = header + script + error_handling
    script_name = os.path.join(logdir, "{}_{}.py".format(job_name, get_uuid()))
    with open(os.path.expanduser(script_name), "w") as fp:
        fp.write(python_script)
    # Note: this seems a better way to start jobs, but I utterly failed 
    # at getting sbcast to work correctly.
    # subprocess.call(["sbcast", script_name, script_name])    
    # Create slurm command
    slurm_cmd = ['sbatch', 
                 '-c', str(ncpus),
                 '-p', partition,
                 '--mem', str(mem)+'G',
                 #'--mem-per-cpu', str(mem)+'M',
                 '-o', logfile,
                 '-J', job_name]
    if dep is not None:
        slurm_cmd += ['-d', dep]
    if slurm_err is not None:
        slurm_cmd += ['-e', errfile]
    if time_limit is not None:
        slurm_cmd += ['-t', str(time_limit)]
    if account is not None:
        slurm_cmd += ['--account', account]
    if singularity_container is None:
        singularity_line = ''
    else:
        if singularity_drive_mount != '':
            singularity_line = 'SINGULARITY_SHELL=/bin/bash\nsingularity exec -B {mnt} {img} '.format(
                mnt=singularity_drive_mount, img=singularity_container)
        else:
            singularity_line = 'SINGULARITY_SHELL=/bin/bash\nsingularity exec {img} '.format(
                mnt=singularity_drive_mount, img=singularity_container)
    # stdbuf lines force immediate buffer write for better updating
    # of log files during jobs. 
    slurm_script = textwrap.dedent("""
    #!/bin/bash
    #SBATCH
    source ~/.bashrc
    {singularity_line}stdbuf -o0 -e0 {cmd} {script_name}
    echo "Job finished! cleanup:"
    echo "removing {script_name}"
    rm {script_name}
    """)[1:].format(cmd=cmd, 
                    singularity_line=singularity_line, 
                    script_name=script_name,
                    )
    #print('=== Calling: ===')
    #print(slurm_script)
    if remote_host is None:
        # Call slurm
        clust = subprocess.Popen(slurm_cmd,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        stdout, stderr = clust.communicate(slurm_script.encode())
    else:
        # Start SSH process to remote host
        sshproc = subprocess.Popen(['ssh', remote_host],
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False)

        # Write script to file
        sbatch_file = script_name.replace('.py', '.sh')
        #sbatch_file = os.path.join(logdir, sbatch_file)
        sbatch_file_local = os.path.expanduser(sbatch_file)
        with open(sbatch_file_local, 'w') as fid:
            fid.write(slurm_script)
        # Call slurm sbatch command
        stdout, stderr = sshproc.communicate(' '.join(slurm_cmd + [sbatch_file]).encode('utf-8'))
        # Alternative: Handle with fancy use of subprocess, one less file written to disk.
        # print('Calling slurm command via write to stdin:')
        # print(slurm_cmd)
        # slurm_cmd_bytes = ' '.join(slurm_cmd).encode('utf-8')
        # _ = sshproc.stdin.write(slurm_cmd_bytes)
        # print('Providing input via communicate():')
        # print(slurm_script)
        # stdout, stderr = sshproc.communicate(slurm_script.encode('utf-8'))

        # Done calling job

    # Wrap-up: retrieve job ID or show error
    try:
        job_id = re.search('(?<=Submitted batch job )[0-9]*', stdout.decode()).group()
    except:
        print('=== Stdout: ===')
        print(stdout.decode())
        raise Exception('Slurm job submission failed!\n=== Stderr: ===\n%s'%stderr.decode())
    return job_id

def run_function(fn, jp, *args, **kwargs):
    """Run a function on slurm queue w/ fancier calling syntax

    Inputs are simply the function to call and its args & kwargs. A slightly more
    convenient wrapper for run_script.

    Parameters
    ----------
    fn : function
        function to be called
    jp : dict
        Parameters governing slurm job. Fields are:
        ncpus : positive int < 4, default=2
            number of CPUs to request on slurm
        mem : positive int
            memory in mb, default=7700
        dep : string job id(s) or None
            string job numbers for dependencies, e.g. '78823, 78824, 78825' (for 
            3 slurm job dependencies with those job id numbers). default=None
        module : string name for module
            name for module in which `fn` lives; useful if `fn` is defined 
            in the script in which it is called
        logdir : string path for logging directory
            name for logging directory
        slurm_out : string output log file name
            file name for normal log file
        slurm_err : string error file name
            file name for error log file (only generated if there is an error)
    args : arguments to pass to `fn`, passed individually
        All args must be serializable (no fancy objects...)
    kwargs : keyword arguments to pass to `fn`, plus other args (see below)
        Same limitation as `args`: all keys/values must be serializable
    

    Notes
    -----
    Be careful with your imports: if you define the function called here (`fn`) 
    within a file that has executable code in it, that code will also be 
    executed as you import the function from that file/module. 

    """
    u = get_uuid()
    now = time.strftime('%Y_%m_%d_%H%M', time.localtime())
    job_name = fn.__name__+'_%s_%s'%(now, u)
    # Handle specific named inputs
    if jp is None:
        jp = {}
    default_params = dict(ncpus=3, mem=30, dep=None, module=fn.__module__, 
        logdir=default_logdir, slurm_out='%s_%s_'%(fn.__name__, now)+'%j_%N.out',
        slurm_err='ErrorFile_%s_%s_'%(fn.__name__, now)+'%j_%N.out',
        )
    default_params.update(jp)
    jp = default_params
    module = jp.pop('module')
    # Get temporary file name, save all inputs
    var_file = os.path.join(jp['logdir'], 'TempSlurmVars_%s'%time.strftime('%Y_%m_%d_%H%M_')+u+'.pik')
    with open(var_file, 'wb') as fid:
        pickle.dump({'args':args, 'kwargs':kwargs}, fid)
    # (2) create string to load arguments back, call function
    script = """# Auto-generated slurm script!
import pickle
import os
import sys
import matplotlib         # This is annoying overhead, but we have to make sure
matplotlib.use('Agg') # we're not assuming an X display on cluster machines
# Print lots of debugging info
print('Current directory:')
print(os.path.abspath(os.path.curdir))
print('Importing:')
if not '/' in '{module}':
    print('from {module} import {func_name}')
    #from {module} import {func_name}
    {func_name} = __import__('{module}', globals(), locals(), '{func_name}').{func_name}
else:
    pth, mod = os.path.split('{module}')
    mod, ext = os.path.splitext(mod)
    print('Appending %s to path...'%(pth))
    sys.path.append(pth)
    print('from %s import {func_name}'%(mod))
    {func_name} = __import__(mod, globals(), locals(), '{func_name}').{func_name}
# Load variables
print('Loading {var_file}')
with open('{var_file}', mode='rb') as fid:
    vars = pickle.load(fid)
print('Calling function!')
# Call function
{func_name}(*vars['args'], **vars['kwargs'])
# Cleanup variables
print('Cleanup -> removing {var_file}')
os.unlink('{var_file}')
""".format(func_name=fn.__name__, module=module, var_file=var_file)
    # (3) Call Slurm
    job_id = run_script(script, **jp)
    return job_id


def remote_rsync(source_folder,
                 destination_folder,
                 remote_host,
                 rsync_options='-rv',
                 ssh_agent_path='~/Code/ssh-find-agent/ssh-find-agent.sh',
                 ):
    # Set up commands to call & securely handle passwords
    #rsync_pw = getpass.getpass('Please enter rsync password to destination folder computer: ')
    #rsync_pw_command = "export RSYNC_PASSWORD=%s\n"%rsync_pw
    rsync_command = ' '.join(
        ['rsync', rsync_options, source_folder, destination_folder]) + '\n'
    # Start SSH process to remote host
    sshproc = subprocess.Popen(['ssh', '-tt', remote_host],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=False,
                               #env=dict(RSYNC_PASSWORD=rsync_pw)
                               #universal_newlines=True,
                               #bufsize=0)
                               )

    # Call slurm sbatch command
    # rsync password setup...
    sshproc.stdin.write(
        ("source %s\n"%ssh_agent_path).encode('utf-8'))
    sshproc.stdin.write("set_ssh_agent_socket\n".encode('utf-8'))
    #sshproc.stdin.write(rsync_pw_command.encode('utf-8'))
    sshproc.stdin.write("echo $SSH_AGENT_PID\n".encode('utf-8'))
    #sshproc.stdin.write("echo $SUBJECTS_DIR\n".encode('utf-8'))
    sshproc.stdin.write(rsync_command.encode('utf-8'))
    sshproc.stdin.write("logout\n".encode('utf-8'))
    sshproc.stdin.close()
    # Record outputs & errors
    stdout = []
    for line in sshproc.stdout:
        txt = line.decode()
        to_exclude = remote_host.split('.')[0]
        #if txt == rsync_pw_command.replace('\n','\r\n'):
        #    continue
        if to_exclude not in txt:
            stdout.append(txt)
    stderr = []
    for line in sshproc.stderr:
        stderr.append(line.decode())

    return stdout, stderr

def scatter(db, dbID, n, fn, *args, **kwargs):
    """Scatter jobs to slurm queue

    WIP!

    `fn` must take db, dbID, and n as its first 3 arguments.
    
    """
    jid = []
    for nn in n:
        jid.append(run_slurm(fn, db, dbID, nn, *args, **kwargs))
    return jid
