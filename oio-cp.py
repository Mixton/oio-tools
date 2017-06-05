#!/usr/bin/python

# run by crontab

import os, sys, time
import io
import re
import argparse
import math
import smtplib
import textwrap
import logging
import json
import multiprocessing
import mimetypes
import signal
import collections
import magic
from PIL import Image
from PIL.ExifTags import TAGS
from io import BytesIO
from random import randint
from oio.api.object_storage import ObjectStorageAPI

class Consumer(multiprocessing.Process):

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            next_task = self.task_queue.get()
            if next_task is None:
                # Poison pill means shutdown
                #print '%s: Exiting' % proc_name
                logger = logging.getLogger('openioCP')
                logger.debug('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            #print '%s: %s' % (proc_name, next_task)
            answer = next_task()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return


class Task(object):
    def __init__(self, length, obj, oioproxy, oions, oioaccount, oiocontainer, obj_name):
        self.length = length
        self.obj = obj
        self.obj_name = obj_name
        self.oioproxy = oioproxy
        self.oions = oions
        self.oioaccount = oioaccount
        self.oiocontainer = oiocontainer
    def __call__(self):
        return cp_files(self.length, self.obj, self.oioproxy, self.oions, self.oioaccount, self.oiocontainer, self.obj_name)
    def __str__(self):
        return '%s' % (self.length)

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True

def cp_files(length, obj_path, oioproxy, oions, oioaccount, oiocontainer, obj_name):
    storage = ObjectStorageAPI(oions, "http://%s:6006"%oioproxy)
    #container = "livraison-"+str(length)
    container = oiocontainer
    logger = logging.getLogger('openioCP')
    start = time.time()
    content_type = magic.from_file(obj_path, mime=True)
    metadata = {}
    metadata_rm = ( 'MakerNote', 'UserComment')
    
    if 'image' in content_type:
        i = Image.open(obj_path)
        info = i._getexif()
        if info:
            for tag, value in info.items():
                decoded = TAGS.get(tag, tag)
                if not isinstance( decoded, int ):
                    if not any (meta_rm in decoded for meta_rm in metadata_rm):
                        metadata[str(decoded)] = str(value)

    try:
        #print(obj_path)
        logger.info('%s'%obj_path)
        f = io.open(obj_path, "rb")
        #obj = storage.object_create(oioaccount, container,  f, obj_name=obj_name, content_length=length)
        obj = storage.object_create(oioaccount, container,  f, content_type=content_type, mime_type=content_type, obj_name=obj_name, content_length=length, metadata=metadata)
        #obj = storage.object_create(oioaccount, container,  f, mime_type=content_type, obj_name=obj_name, metadata=json.dumps(metadata), content_length=length)
    except Exception as e:
        #print "error"
        #print e
        logger.warn('%s'%str(e))
        results = {
                'status': "nok",
                'size': length,
                'error': str(e)
        }
        return results
    else:
        delay = time.time() - start
        #print("%s written in %5.3f ms"%(convert_size(length),delay*1000))
        logger.info("%s written in %5.3f ms"%(convert_size(length),delay*1000))
        results = {
                'status': "ok",
                'size': length,
                'rt' : delay
        }
        return results

def get_file_directory(file):
    return os.path.dirname(os.path.abspath(file))

def convert_size(size):
    if (size == 0):
        return '0B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size,1024)))
    p = math.pow(1024,i)
    s = round(size/p,2)
    return '%s %s' % (s,size_name[i])

def list_all_files(tasks, dir, oions, oioaccount, oioproxy, oiocontainer):
    root_path = dir
    killer = GracefulKiller()
    oioproxy_list = randint(0,len(oioproxy)-1)
    storage = ObjectStorageAPI(oions, "http://%s:6006"%oioproxy[oioproxy_list])

    num_jobs = 0
    num_files = 0

    container = oiocontainer

    tasks = {}

    for root, dirnames, filenames in os.walk(root_path):
        for filename in filenames:
            if os.path.isfile(os.path.join(root, filename)):
                #print os.path.join(root, filename)
                oioproxy_job = randint(0,len(oioproxy)-1)
                t = os.stat(os.path.join(root, filename))
                c = t.st_mtime
                size = t.st_size
                path = os.path.join(root, filename)
                #container = "livraison-"+str(size)
                num_files += 1
                obj_name = path.replace(dir,"")
                try:
                    meta, stream = storage.object_fetch(oioaccount, container, obj_name)
                except Exception as e:
                    tasks[num_jobs] = {
                            'size': size,
                            'path': path,
                            'name': obj_name,
                            'oioproxy': oioproxy[oioproxy_job],
                            'oions': oions,
                            'oioaccount': oioaccount,
                            'oiocontainer': container
                    }
                    num_jobs += 1
                else:
                    if int(meta['length']) != size:
                        print("%i %i"%(int(meta['length']),size))
                        tasks[num_jobs] = {
                                'size': size,
                                'path': path,
                                'name': obj_name,
                                'oioproxy': oioproxy[oioproxy_job],
                                'oions': oions,
                                'oioaccount': oioaccount,
                                'oiocontainer': container
                        }
                        num_jobs += 1
                sys.stdout.write("\rjobs: %i/%i, current path: %s" % (num_jobs, num_files, path))
                sys.stdout.flush()

            if killer.kill_now:
                print("killing jobs %i"%num_jobs)
                break
        if killer.kill_now:
            print("killing jobs %i"%num_jobs)
            break

    return tasks


def logwarn():
    return logging.WARNING

def loginfo():
    return logging.INFO

def logdebug():
    return logging.DEBUG

def main():

    now = time.time()

    old_stdout = sys.stdout
    loglevel = logging.ERROR
    result = {}


    tasks = multiprocessing.JoinableQueue()
    results = multiprocessing.Queue()

    #killer = GracefulKiller()

    # arguments parsing
    parser = argparse.ArgumentParser(prog='oio-cp', formatter_class=argparse.RawDescriptionHelpFormatter, description=textwrap.dedent('''\
                        '''), epilog="e.g. oio-cp.py -d /foo/bar/ --ns=OPENIO --account=ACCOUNT_OPENIO --proxy=proxy1,proxy2,proxy3")

    parser.add_argument('-w', '--workers', help='set worker num', required=False, default=multiprocessing.cpu_count())
    parser.add_argument('-d', '--dir', help='directory to copy', required=True)
    parser.add_argument('-o', '--outputfile', help='set output file', required=False)
    parser.add_argument('--ns', help='set oio-ns', required=True)
    parser.add_argument('--account', help='set oio-account', required=True)
    parser.add_argument('--container', help='set oio-account', required=True)
    parser.add_argument('--proxy', help='list of oio-proxy (comma separated)', required=True)
    parser.add_argument('--flatns', help='flatns not available (WIP)', required=False)
    parser.add_argument('--verbose', '-v', action='count')
    parser.add_argument('--version', action='version', version='%(prog)s v1.1')

    args = parser.parse_args()

    #print args.verbose

    switchlogger = {
            1: logwarn,
            2: loginfo,
            3: logdebug,
    }

    funcloglevel = switchlogger.get(args.verbose, lambda: logging.ERROR)

    loglevel = funcloglevel()

    logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

    if args.outputfile:
        log_file = open(args.outputfile,"w")
        sys.stdout = log_file

    oions = args.ns
    oioaccount = args.account
    oiocontainer = args.container
    oioproxy = args.proxy.split(",")

    nbtot = 0
    sizetot = 0

     # Start workers
    num_consumers = int(args.workers)
    print 'Creating %d workers' % num_consumers
    consumers = [ Consumer(tasks, results)
                  for i in xrange(num_consumers) ]

    tassks = list_all_files(tasks, args.dir, oions, oioaccount, oioproxy, oiocontainer)

    num_jobs = len(tassks)


    for ident, task in tassks.iteritems():
        #print task['path']
        tasks.put(Task(task['size'], task['path'], task['oioproxy'], task['oions'], task['oioaccount'], task['oiocontainer'], task['name']))
        #print 'ok'

    start_test = time.time()

    for w in consumers:
        w.start()

    for i in xrange(num_consumers):
        tasks.put(None)

    while tasks.qsize() - num_consumers > 0:
        progress = num_jobs - (tasks.qsize() - num_consumers)
        sys.stdout.write("\rprogress: %i/%i (%i%%)" % (progress, num_jobs, (100 * float(progress)/float(num_jobs))))
        sys.stdout.flush()
        time.sleep(1)

    tasks.join()

    for i in xrange(num_consumers):
        tasks.put(None)

    sys.stdout.write("\rprogress: %i/%i (100%%)" % (num_jobs, num_jobs))
    sys.stdout.flush()

    test_delay = time.time() - start_test

    print("\nretrieving stats...")

    obj_stocked = 0
    obj_failed = 0
    rt = 0
    sizes = collections.Counter()
    size = 0
    sizes_ok = collections.Counter()
    obj_created = num_jobs
    errors = collections.Counter()
    while num_jobs:
        result = results.get()
        if result is not None:
            if result['status'] == "ok":
                obj_stocked += 1
                size += result['size']
                rt += result['rt']
                sizes_ok[str(result['size'])] += 1
            if result['status'] == "nok":
                obj_failed += 1
                errors[str(result['error'])] += 1
            sizes[str(result['size'])] += 1
        num_jobs -= 1

    print("%i containers"%len(sizes_ok))
    print("%i objects written"%obj_stocked)
    if obj_created > 0:
        print("%i objects failed (%i%%)"%(obj_failed, int(100 * float(obj_failed)/float(obj_created))))
    #obj_ps = obj_created/test_delay
    obj_ps = obj_stocked/test_delay
    error_ps = obj_failed/test_delay
    print("- %5.1f objects/s"%obj_ps)
    print("- %5.1f fails/s"%error_ps)
    for error_type, error_nb in errors.iteritems():
        print("-- %s: %i"%(error_type, error_nb))
    print("%s written in %5.3fs"%(convert_size(size), test_delay))
    octet_ps = size/test_delay
    print("%s/s"%convert_size(int(octet_ps)))
    if obj_stocked > 0:
        print("RT avg. %5.3fms"%(1000 * rt/obj_stocked))


    if args.outputfile:
        sys.stdout = old_stdout
        log_file.close()


if __name__ == "__main__":
    main()
