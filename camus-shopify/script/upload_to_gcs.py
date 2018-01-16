#####
# Upload directories imported by Camus to Google Cloud
#####
import argparse
import os
import subprocess
import logging


HDFS_PREFIX = "hdfs://hadoop-production"
GCS_PREFIX = "gs://raw-kafka"
MIN_FILES_FOR_DIST = 8  # use regular copy for small number of files
DIST_MAPPERS = 32  # allowed parallelism for distributed copy
DIST_QUEUE = 'production.ignore'  # yarn queue for distributed copy
DIST_MEM_MB = 4000  # memomory limit for distributed copy
UPLOADED_FLAG = "_UPLOADED"  # flag to place inside a camus execution folder to mark it as fully uploaded to gcs


logger = logging.getLogger('Camus GCS upload')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(levelname)s - %(message)s')


class HDFS(object):
    @staticmethod
    def ls(path):
        result = subprocess.check_output(['hadoop', 'fs', '-ls', '-C', path])
        return result.split()

    @staticmethod
    def last_camus_executions(path, n=24):
        result = subprocess.check_output('hadoop fs -ls -C {path} | sort -r | head -n {n}'.format(path=path, n=n),
                                         shell=True)
        return result.split()

    @staticmethod
    def all_dropped_dirs(camus_exec):
        subprocess.check_output('hadoop fs -cat {camus_exec}/dirs-written-to-* | '
                                'sort | '
                                'uniq | '
                                'hadoop fs -put -f - {camus_exec}/dirs-written-to__all.txt'
                                .format(camus_exec=camus_exec),
                                shell=True)
        result = subprocess.check_output('hadoop fs -cat {camus_exec}/dirs-written-to__all.txt'
                                         .format(camus_exec=camus_exec),
                                         shell=True)
        return result.split()

    @staticmethod
    def count_files(path):
        result = subprocess.check_output('hadoop fs -ls -C {path} | wc -l'.format(path=path), shell=True)
        return int(result)

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


def camus_execution_path(path):
    with open(path, "r") as f:
        tups = [
            tuple(line.split("="))
            for line in f.readlines() if line.strip() and not line.startswith("#")
        ]
    properties = {t[0].strip(): t[1].strip() for t in tups}
    return properties['etl.execution.history.path']


def upload_camus_execution(camus_exec_folder, topic_whitelist):
    # check if _UPLOADED flag is present -- only upload if it's not
    all_paths = HDFS.ls(camus_exec_folder)
    filenames = set(os.path.split(p)[1] for p in all_paths)
    if UPLOADED_FLAG in filenames:
        logger.info("Camus folder {} is already uploaded, skipping".format(camus_exec_folder))
        return True
    else:
        logger.info("Uploading dirs dropped by {camus_exec} execution".format(camus_exec=camus_exec_folder))
        failures = upload_all_dropped_folders(camus_exec_folder, topic_whitelist)
        if failures:
            logger.error("Some directories failed to upload correctly: {}".format(failures))
            return False
        return True


def upload_all_dropped_folders(exec_folder, topic_whitelist):
    failures = []
    dropped_dirs = HDFS.all_dropped_dirs(exec_folder)
    for dd in dropped_dirs:
        # hacky way to get topic assuming data is always partitioned by hour
        topic = dd.split("/")[-5]
        if topic not in topic_whitelist:
            logger.info("Topic {topic} is not whitelisted, skipping {dd}".format(topic=topic, dd=dd))
        else:
            all_uploaded = upload_dropped_dir(dd)
            if not all_uploaded:
                failures.append(dd)
    return failures


def upload_dropped_dir(data_directory):
    # use regular upload for small number of files
    num_files = HDFS.count_files(data_directory)
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
    return check_all_uploaded(data_directory, destination)


def check_all_uploaded(src, dest):
    paths_on_hdfs = set(p.replace(HDFS_PREFIX, "") for p in HDFS.ls(src))
    paths_on_gcs = set(p.replace(GCS_PREFIX, "") for p in HDFS.ls(dest))
    logger.info("Checking uploaded dirs {src} <-> {dest}".format(src=src, dest=dest))
    not_in_gcs = paths_on_hdfs - paths_on_gcs
    if not_in_gcs:
        logger.error("Missing files in gcs: {}".format(not_in_gcs))
        return False
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload Camus directories to GCS')
    parser.add_argument('properties', type=str, help='Absolute path to Camus properties file')
    parser.add_argument('whitelist', type=str, help='Absolute path to the file containing topics to upload')
    parser.add_argument('executions', type=int, help='Number of camus executions to look at')
    args = parser.parse_args()

    # Check the arguments are valid
    if not (os.path.isabs(args.properties) and os.path.isfile(args.properties)):
        logger.error("Camus properties file path is not valid: {}".format(args.properties))
        exit(1)
    if not (os.path.isabs(args.whitelist) and os.path.isfile(args.whitelist)):
        logger.error("Topic whitelist path is not valid: {}".format(args.properties))
        exit(1)

    # Get Camus execution folder
    camus_exec_folder = camus_execution_path(args.properties)
    logger.info("Camus execution history folder: {}".format(camus_exec_folder))

    # List the last 24 executions (just to be safe, some uploads might have failed)
    camus_exec_dirs = HDFS.last_camus_executions(camus_exec_folder, args.executions)

    if not camus_exec_dirs:
        logger.warn("No Camus executions in history folder: {}".format(camus_exec_folder))

    else:
        topic_whitelist = topics_to_upload(args.whitelist)
        logger.info("Whitelisted topics: {}".format(topic_whitelist))

        upload_failure = False
        for exec_dir in camus_exec_dirs:
            upload_success = upload_camus_execution(exec_dir, topic_whitelist)
            if upload_success:
                # mark the execution folder as fully uploaded if all folders it dropped uploaded successfully
                HDFS.touch(os.path.join(exec_dir, UPLOADED_FLAG))
            else:
                upload_failure = True

        if upload_failure:
            exit(1)
