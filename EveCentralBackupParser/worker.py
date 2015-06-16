import threading
import sys
import urllib2


class Worker(threading.Thread):
    def __init__(self, gts):
        threading.Thread.__init__(self)
        self.gts = gts

    def run(self):
        found = []
        for gt in self.gts:
            try:
                pass
            except urllib2.HTTPError as e:
                return e
            found.append(gt)
        print found

    def main():
        thread_max = int(sys.argv[2])
        gts = []
        jobs = []

        worker_tag_start_points = [i for i in xrange(0, len(gts) + 1, len(gts) / thread_max)]
        for i in xrange(thread_max):
            t = Worker(gts[worker_tag_start_points[i]:worker_tag_start_points[i + 1]])
            t.start()
            jobs.append(t)

        for job in jobs:
            job.join()

main()
