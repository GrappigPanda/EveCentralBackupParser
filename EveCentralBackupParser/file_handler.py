from os import chdir, remove
from glob import glob
import gzip
import threading


class Worker(threading.Thread):
    def __init__(self, files, max_threads=10):
        threading.Thread.__init__(self)
        self.thread_max = max_threads
        self.files = files

    def run(self):
        for f in self.files:
            print f
            try:
                extract(f)
            except IOError as e:
                print e
                return None
            except Exception as e:
                print e
                return None


def enumerate_files(dir):
    chdir(dir)
    return [file for file in glob("./*.dump.gz")]


def retrieve_file_lines(f):
    for line in iter(f.readline, b''):
        yield line


def parse_line(line):
    a = line.replace('"', '').replace(" ", "")
    a = a.split(',')
    if a[2] == 30000142 or a[2] == "30000142":
        i = [a[2], a[4], a[6], a[14]]
        return ' '.join([x for x in i])
    else:
        return None


def extract(file):
    fout = open("dump", "w")
    with gzip.open(file, "rb") as f:
        for line in f:
            linetmp = parse_line(line)
            if linetmp is None:
                continue
            fout.write(linetmp)
            fout.write("\n")
        fout.close()
    f.close()

    remove(file)


def worker_bounds(files, thread_max):
    files_len = len(files)
    files_per_thread = files_len / thread_max

    bounds = [(i * files_per_thread,
              i * files_per_thread + files_per_thread - 1)
              for i in xrange(thread_max)]

    return bounds


def main():
    thread_max = 10
    files = []
    jobs = []

    files = enumerate_files("E:/EveDump/EveCentralDataDump/eve-central.com/dumps")

    bounds = worker_bounds(files, thread_max)

    for i in xrange(thread_max):
        t = Worker(files[bounds[i][0]:bounds[i][1]])
        t.start()
        jobs.append(t)

    for job in jobs:
        job.join()


if __name__ == '__main__':
    main()
