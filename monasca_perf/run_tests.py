import subprocess
import sys

def main():
    with open("inital_disk", "w") as stdout:
        subprocess.Popen("./disk.sh", shell=True, stdout=stdout)
    kafka_process = subprocess.Popen("exec ./kafka_topics.sh", shell=True)
    disk_process = subprocess.Popen("exec ./disk_writes.sh", shell=True)
    top_process = subprocess.Popen("exec ./top.sh", shell=True)

    try:
	print 'Waiting for keyboard interrupt'
        kafka_process.wait()
    except KeyboardInterrupt:
        with open("final_disk", "w") as stdout:
            subprocess.Popen("./disk.sh", shell=True, stdout=stdout)
        kafka_process.kill()
        disk_process.kill()
        top_process.kill()
	print 'Finished killing processes'

if __name__ == "__main__":
    sys.exit(main())

