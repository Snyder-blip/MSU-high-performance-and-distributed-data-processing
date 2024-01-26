import subprocess
import sys
import argparse
import signal

parser = argparse.ArgumentParser(description='Starter')
parser.add_argument('--x', type=float, default=0.0, help='Initial value for x')
parser.add_argument('--y', type=float, default=0.0, help='Initial value for y')
parser.add_argument('--z', type=float, default=0.0, help='Initial value for z')
parser.add_argument('--N', type=int, default=1, help='Initial value for N')
parser.add_argument('--k', type=int, default=1, help='Initial value for k')


def initialize_variables(x, y, z):
    variables = {'x': x, 'y': y, 'z': z}
    return variables


args = parser.parse_args()
variables = initialize_variables(args.x, args.y, args.z)
N = args.N
k = args.k

processes = []


def signal_handler(sig, frame):
    global processes
    print("Ctrl+C detected. Terminating subprocesses...")
    for process in processes:
        process.terminate()
    sys.exit(1)


signal.signal(signal.SIGINT, signal_handler)

while True:
    print(f'Running primary node...')
    processes.append(subprocess.Popen(
        [sys.executable, 'Primary Node.py', str(variables['x']), str(variables['y']), str(variables['z'])]))

    print(f'Running simulator node...')
    processes.append(subprocess.Popen(
        [sys.executable, 'Simulator Node.py', str(N), str(k)]))

    print(f'Running {k} replicas...')
    for id in range(k):
        processes.append(subprocess.Popen(
            [sys.executable, 'Replica Node.py', str(variables['x']), str(variables['y']), str(variables['z']), f'{id}']))

    for process in processes:
        process.wait()
