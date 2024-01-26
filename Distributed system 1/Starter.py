import subprocess
import sys

broker_host, file_name, storage_procs = sys.argv[1:]
storage_procs = int(storage_procs)

processes = []

processes.append(subprocess.Popen(
    [sys.executable, 'ETL.py', broker_host, file_name, str(storage_procs)]))
processes.append(subprocess.Popen(
    [sys.executable, 'Manager.py', broker_host, str(storage_procs)]))
for i in range(storage_procs):
    processes.append(subprocess.Popen(
        [sys.executable, 'Storage Process.py', broker_host, '{id}'.format(id=i)]))

for process in processes:
    process.wait()
