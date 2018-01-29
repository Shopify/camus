#####
# Upload directories imported by Camus to Google Cloud
#####
import argparse
import os
import subprocess
import logging
import time
from collections import namedtuple

import statsd

HDFS_PREFIX = "hdfs://hadoop-production"
GCS_PREFIX = "gs://raw-kafka"
MIN_FILES_FOR_DIST = 8  # use regular copy for small number of files
DIST_MAPPERS = 32  # allowed parallelism for distributed copy
DIST_QUEUE = 'production.ignore'  # yarn queue for distributed copy
DIST_MEM_MB = 4000  # memory limit for distributed copy
UPLOADED_FLAG = "_UPLOADED"  # flag to place inside a camus execution folder to mark it as fully uploaded to gcs


logger = logging.getLogger('Camus GCS upload')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(levelname)s - %(message)s')

UploadResult = namedtuple('DroppedDirUploadResult', 'upload_list failure_list')


def with_stderr(argument):
    def decorator(func):
        def file_wrapper(*args, **kwargs):
            with open(argument, 'w') as f:
                return func(*args, stderr=f, **kwargs)

        return file_wrapper

    return decorator


class Statsd(object):
    def __init__(self, properties, prefix='Camus'):

        self.host = None
        self.port = None
        self.prefix = prefix
        self.enabled = properties.get('statsd.enabled', False)
        if self.enabled:
            self.host = properties['statsd.host']
            self.port = properties['statsd.port']

        self._client = None

    @property
    def client(self):
        if self._client is None and self.enabled:
            self._client = statsd.StatsClient(self.host, self.port, prefix=self.prefix)
        return self._client

    def gauge(self, metric, value):
        if self.enabled:
            self.client.gauge(metric, value)
        else:
            logger.warn("Cannot send metric, statsd is disabled. Check configuration.")


class HDFS(object):
    @staticmethod
    @with_stderr(os.devnull)
    def ls(path, stderr=None):
        result = subprocess.check_output(['hadoop', 'fs', '-ls', '-C', path], stderr=stderr)
        return result.split()

    @staticmethod
    @with_stderr(os.devnull)
    def last_camus_executions(path, n=24, stderr=None):
        result = subprocess.check_output('hadoop fs -ls -C {path} | sort -r | head -n {n}'.format(path=path, n=n),
                                         shell=True, stderr=stderr)
        return result.split()

    @staticmethod
    @with_stderr(os.devnull)
    def all_dropped_dirs(camus_exec, stderr=None):
        subprocess.check_output('hadoop fs -cat {camus_exec}/dirs-written-to-* | '
                                'sort | '
                                'uniq | '
                                'hadoop fs -put -f - {camus_exec}/dirs-written-to__all.txt'
                                .format(camus_exec=camus_exec),
                                shell=True, stderr=stderr)
        result = subprocess.check_output('hadoop fs -cat {camus_exec}/dirs-written-to__all.txt'
                                         .format(camus_exec=camus_exec),
                                         shell=True, stderr=stderr)
        return result.split()

    @staticmethod
    def mkdir(path):
        subprocess.check_output('hadoop fs -mkdir -p {path}'.format(path=path), shell=True)

    @staticmethod
    def cp(src, dest):
        try:
            subprocess.check_output('hadoop fs -cp {src}/* {dest}'.format(src=src, dest=dest), shell=True)
        except:
            # this fails when some files already exist and we can't differentiate that from any other failure
            # however, it will still upload files that do not exist
            # we manually check if the files were uploaded correctly so it's "ok" to fail here
            pass

    @staticmethod
    def distcp(src, dest, mappers=DIST_MAPPERS, queue=DIST_QUEUE, mem=DIST_MEM_MB):
        cmd = ('hadoop distcp '
               '-Dmapreduce.job.queuename={queue} '
               '-Dmapreduce.map.memory.mb={mem} '
               '-m {mappers} '
               '-update '
               '{src} {dest}'
               .format(src=src, dest=dest, queue=queue, mem=mem, mappers=mappers))
        logger.info('Calling "{}"'.format(cmd))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        output, error = process.communicate()
        logger.info(output)

    @staticmethod
    def touch(path):
        subprocess.check_output('hadoop fs -touchz {path}'.format(path=path), shell=True)


def topics_to_upload(path):
    with open(path, "r") as f:
        topics = [line.strip().replace("_", ".") for line in f.readlines() if line.strip()]
    return topics


def camus_properties_dict(path):
    with open(path, "r") as f:
        tups = [
            tuple(line.split("="))
            for line in f.readlines() if line.strip() and not line.startswith("#")
        ]
    properties = {t[0].strip(): t[1].strip() for t in tups}
    return properties


def upload_camus_execution(camus_exec_folder, topic_whitelist, ignore_dirs):
    # check if _UPLOADED flag is present -- only upload if it's not
    all_paths = HDFS.ls(camus_exec_folder)
    filenames = set(os.path.split(p)[1] for p in all_paths)
    if UPLOADED_FLAG in filenames:
        logger.info("Camus folder {} is already uploaded, skipping".format(camus_exec_folder))
    else:
        logger.info("Uploading dirs dropped by {camus_exec} execution".format(camus_exec=camus_exec_folder))
        result = upload_all_dropped_folders(camus_exec_folder, topic_whitelist, ignore_dirs)
        if result.failure_list:
            logger.error("Some directories failed to upload correctly: {}".format(result.failure_list))
        return result


def upload_all_dropped_folders(exec_folder, topic_whitelist, ignore_dirs):
    successes, failures = set(), set()
    dropped_dirs = HDFS.all_dropped_dirs(exec_folder)
    for dd in dropped_dirs:
        try:
            topic = dd.split("/")[-5]  # hacky way to get topic assuming data is always partitioned by hour
            if dd in ignore_dirs:
                logger.info("Skipping {}".format(dd))  # already uploaded in this run
            if topic not in topic_whitelist:
                logger.info("Topic {topic} is not whitelisted, skipping {dd}".format(topic=topic, dd=dd))
            else:
                all_uploaded = upload_dropped_dir(dd)
                (successes if all_uploaded else failures).add(dd)
        except Exception:
            logger.error("Could not upload {}".format(dd))
            failures.add(dd)
    if not failures:
        # mark the execution folder as fully uploaded if all folders it dropped uploaded successfully
        HDFS.touch(os.path.join(exec_dir, UPLOADED_FLAG))
    return UploadResult(upload_list=successes, failure_list=failures)


def upload_dropped_dir(data_directory):
    # use regular upload for small number of files
    filelist = HDFS.ls(data_directory)  # we will use these to check the upload
    num_files = len(filelist)
    destination = data_directory.replace(HDFS_PREFIX, GCS_PREFIX)
    HDFS.mkdir(destination)  # create the parent folder if does not exist
    if num_files < MIN_FILES_FOR_DIST:
        logger.info("Copying using regular copy {src} to {dest}".format(src=data_directory, dest=destination))
        HDFS.cp(data_directory, destination)
    else:
        logger.info("Copying using distributed copy {src} to {dest}".format(src=data_directory, dest=destination))
        num_mappers = min(num_files, DIST_MAPPERS)
        HDFS.distcp(data_directory, destination, queue=DIST_QUEUE, mappers=num_mappers, mem=DIST_MEM_MB)
    # Sanity check to make sure all files were uploaded as distcp can be flaky
    logger.info("Checking uploaded files in {dest}".format(dest=destination))
    missing = not_uploaded(destination, filelist)
    if missing:
        logger.error("Missing files in gcs for {dest}: {files}".format(dest=destination, files=missing))
    return len(missing) == 0


def not_uploaded(dest, filelist):
    # this does not guarantee that the contents are identical since there may be files written to the source dir
    # while our copy is taking place, but it's best effort in terms of at the very least the files that were there
    # before we started the copy should be copies over
    paths_on_hdfs = set(p.replace(HDFS_PREFIX, "") for p in filelist)
    paths_on_gcs = set(p.replace(GCS_PREFIX, "") for p in HDFS.ls(dest))
    return paths_on_hdfs - paths_on_gcs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload Camus directories to GCS')
    parser.add_argument('properties', type=str, help='Absolute path to Camus properties file')
    parser.add_argument('whitelist', type=str, help='Absolute path to the file containing topics to upload')
    parser.add_argument('--executions', type=int, help='Number of latest camus executions to look at')
    parser.add_argument('--execution', type=str, help='Specific execution directory to upload data for')
    args = parser.parse_args()

    start = time.time()  # we'll use this to report total run-time

    # Check the arguments are valid
    if not (os.path.isabs(args.properties) and os.path.isfile(args.properties)):
        logger.error("Camus properties file path is not valid: {}".format(args.properties))
        exit(1)

    # Load Camus properties
    properties = camus_properties_dict(args.properties)
    # Setup Statsd
    statsd_client = Statsd(properties)

    try:
        # Get Camus execution folder
        camus_exec_folder = properties['etl.execution.history.path']
        logger.info("Camus execution history folder: {}".format(camus_exec_folder))
        if args.executions:
            # List the last n executions
            camus_exec_dirs = HDFS.last_camus_executions(camus_exec_folder, args.executions)
        elif args.execution:
            camus_exec_dirs = ['{}/{}'.format(camus_exec_folder, args.execution)]
        else:
            logger.error("Either a specific execution(--execution) or "
                         "the number of latest executions (--executions) must be specified".format(args.properties))
            raise Exception("Invalid arguments.")

        # Load the list of topics whitelisted for upload
        if not (os.path.isabs(args.whitelist) and os.path.isfile(args.whitelist)):
            logger.error("Topic whitelist path is not valid: {}".format(args.properties))
            raise Exception("Invalid arguments.")

        topic_whitelist = topics_to_upload(args.whitelist)
        logger.info("Whitelisted topics: {}".format(topic_whitelist))

        upload_failure = False
        dir_count = 0
        ignore_dirs = set()

        exec_dir = None
        for exec_dir in camus_exec_dirs:
            upload_result = upload_camus_execution(exec_dir, topic_whitelist, ignore_dirs)
            # remember which directories were uploaded and don't upload on the next run
            ignore_dirs.update(upload_result.upload_list)

            if not upload_result.failure_list:
                # mark the execution folder as fully uploaded if all folders it dropped uploaded successfully
                HDFS.touch(os.path.join(exec_dir, UPLOADED_FLAG))
                dir_count += len(upload_result.upload_list)
            else:
                logger.error("Some directories failed to upload correctly: {}".format(upload_result.failure_list))
                upload_failure = True
        if exec_dir is None:
            logger.warn("No Camus executions in history folder: {}".format(camus_exec_folder))

        end = time.time()
        runtime_seconds = end - start

        logger.info("Uploaded a total of {n} directories in {t} seconds".format(n=dir_count, t=runtime_seconds))

        statsd_client.gauge('CamusUploadToGCS.failure', int(upload_failure))
        statsd_client.gauge('CamusUploadToGCS.run-time', runtime_seconds)
        statsd_client.gauge('CamusUploadToGCS.dir-count', dir_count)
        exit(int(upload_failure))

    except Exception as e:
        logger.error(e)
        statsd_client.gauge('CamusUploadToGCS.failure', 1)
        exit(1)
