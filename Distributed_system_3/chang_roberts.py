import subprocess
import time
import pika

def send_message(process_id, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=str(process_id))
    channel.basic_publish(exchange='', routing_key=str(process_id), body=message)
    connection.close()

def receive_message(process_id):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=str(process_id))
    method_frame, header_frame, body = channel.basic_get(queue=str(process_id))
    connection.close()
    return body

def chang_and_roberts(process_id, num_processes):
    leader = None

    for i in range(num_processes):
        if i != process_id:
            send_message(i, f"ELECTION {process_id}")

    while True:
        time.sleep(1)
        message = receive_message(process_id)
        if message:
            _, sender_id = message.decode().split()
            sender_id = int(sender_id)
            if sender_id > process_id:
                send_message(sender_id, f"OK {process_id}")
            elif sender_id < process_id:
                send_message(sender_id, f"NO {process_id}")
                break
            else:
                leader = process_id
                break

    print(f"Process {process_id} is the leader. Leader ID: {leader}")

if __name__ == "__main__":
    num_processes = 5

    processes = []
    for i in range(num_processes):
        process = subprocess.Popen(["python", "chang_roberts.py", str(i), str(num_processes)])
        processes.append(process)

    time.sleep(2)  # Allow subprocesses to start

    chang_and_roberts(int(processes[0].args[1]), num_processes)

    # Wait for subprocesses to finish
    for process in processes:
        process.wait()
