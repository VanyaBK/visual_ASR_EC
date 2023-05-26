# -*- coding: utf-8 -*-
"""
Tool class for post-processing youtube download results
Base class contains a time thread that can be used to upload data periodically during the download process.
"""
import os
import shutil
import subprocess
from threading import Timer

from log_settings import get_logger

logger = get_logger("porter")


class BasePorter(Timer):

    def __init__(self, **kwargs):
        self.interval = kwargs.pop('interval', 60)
        self.dir_path = kwargs.pop('dir_path', None) or os.curdir
        self.clean = kwargs.pop('clean', False)
        self.clean_ext = ".mark"
        super(BasePorter, self).__init__(interval=self.interval,
                                         function=self.sync, **kwargs)

    def run(self):
        while not self.finished.is_set():
            self.function(clean=self.clean, **self.kwargs)
            self.finished.wait(self.interval)

    def sync(self, clean=True, **kwargs):
        raise NotImplemented


class StatisticPorter(BasePorter):
    def __init__(self, **kwargs):
        super(StatisticPorter, self).__init__(**kwargs)
        self.cache = set()
        self.static = {
            "id_number": 0,
            "duration(seconds)": 0
        }

    def sync(self, clean=False, **kwargs):
        this_round_static = dict.fromkeys(self.static.keys(), 0)
        logger.warning(
            "StatisticPorter won't delete original file, but won't read clean mark file")
        dir_or_files = os.listdir(self.dir_path)
        for dir_file in dir_or_files:
            tmp = os.path.join(self.dir_path, dir_file)
            if not os.path.isdir(tmp):
                continue
            if dir_file in self.cache:
                continue
            files = os.listdir(tmp)
            for f in files:
                if f.endswith(self.clean_ext) or '-' not in f:
                    continue
                if os.path.isdir(os.path.join(tmp, f)):
                    continue
                t = f.split('-')[0]
                this_round_static["duration(seconds)"] += int(t)
                break
            self.cache.add(dir_file)
            this_round_static["id_number"] += 1
        logger.info("this round statistic info: {}".format(this_round_static))
        for k, v in this_round_static.items():
            self.static[k] += v
        logger.info("total statistic info: {}".format(self.static))



class HdfsPorter(BasePorter):
    def __init__(self, dest_dir, **kwargs):
        self.pack_mode = kwargs.pop("pack_mode", 'tar')
        super(HdfsPorter, self).__init__(**kwargs)
        if dest_dir is None:
            raise Exception('Destination directory should be specified.')

        logger.info(f"Begin sync data from local dir: {self.dir_path} to {dest_dir}")
        directory = f"{dest_dir.rstrip('/')}"
        subprocess.run(f"hadoop fs -mkdir {directory}", shell=True)
        self.directory = directory

    def sync(self, clean=False, **kwargs):
        pack_mode = self.pack_mode
        if os.path.exists(self.dir_path):
            logger.info("begin syncing")
            dir_or_files = os.listdir(self.dir_path)

            for dir_file in dir_or_files:
                tmp = os.path.join(self.dir_path, dir_file)
                if not os.path.exists(os.path.join(tmp, '_SUCCESS')):
                    continue
                if tmp.endswith(self.clean_ext):
                    continue

                assert self.pack_mode in ['tar', 'raw', 'folder'], "pack mode in `tar`, `raw`, `folder`"
                try:
                    if self.pack_mode == 'tar':
                        logger.info(f"compressing {dir_file}")
                        subprocess.run(f"tar -czf {dir_file}.tar -C {self.dir_path} ./{dir_file}", shell=True)
                        finish_flag = subprocess.run(f"hadoop fs -put -f {dir_file}.tar {self.directory}", shell=True).returncode
                        if finish_flag == 0 and clean:
                            os.remove(f"{dir_file}.tar")
                    elif self.pack_mode == 'raw':
                        finish_flag = subprocess.run(f"hadoop fs -put -f {tmp}/* {self.directory}", shell=True).returncode
                    else:  # self.pack_mode == 'folder'
                        finish_flag = subprocess.run(f"hadoop fs -put -f {tmp} {self.directory}", shell=True).returncode
                except Exception as ex:
                    print(ex)
                    finish_flag = -1

                if finish_flag == 0:
                    if clean:
                        logger.info(f"cleaning {tmp}")
                        shutil.rmtree(tmp, ignore_errors=True)
                    else:
                        shutil.move(tmp, tmp + self.clean_ext)


class LocalPorter(BasePorter):
    def __init__(self, dest_dir, **kwargs):
        self.pack_mode = kwargs.pop("pack_mode", 'folder')
        super(LocalPorter, self).__init__(**kwargs)
        if dest_dir is None:
            raise Exception('Destination directory should be specified.')

        logger.info(f"Begin sync data from local dir: {self.dir_path} to {dest_dir}")
        directory = f"{dest_dir.rstrip('/')}"
        os.makedirs(directory, exist_ok=True)
        self.directory = directory

    def sync(self, clean=False, **kwargs):
        if os.path.exists(self.dir_path):
            logger.info("begin syncing")
            dir_or_files = os.listdir(self.dir_path)

            for dir_file in dir_or_files:
                tmp = os.path.join(self.dir_path, dir_file)
                if not os.path.exists(os.path.join(tmp, '_SUCCESS')):
                    continue
                if tmp.endswith(self.clean_ext):
                    continue

                assert self.pack_mode in ['tar', 'raw', 'folder'], "pack mode in `tar`, `raw`, `folder`"
                try:
                    if self.pack_mode == 'tar':
                        logger.info(f"compressing {dir_file}")
                        subprocess.run(f"tar -czf {dir_file}.tar -C {self.dir_path} ./{dir_file}", shell=True)
                        finish_flag = subprocess.run(f"mv {dir_file}.tar {self.directory}", shell=True).returncode
                        if finish_flag == 0 and clean:
                            os.remove(f"{dir_file}.tar")
                    elif self.pack_mode == 'raw':
                        finish_flag = subprocess.run(f"mv {tmp}/* {self.directory}", shell=True).returncode
                    else:  # self.pack_mode == 'folder'
                        finish_flag = subprocess.run(f"mv {tmp} {self.directory}", shell=True).returncode
                except Exception as ex:
                    print(ex)
                    finish_flag = -1

                if finish_flag == 0:
                    if clean:
                        logger.info(f"cleaning {tmp}")
                        shutil.rmtree(tmp, ignore_errors=True)
                    else:
                        shutil.move(tmp, tmp + self.clean_ext)


if __name__ == '__main__':
    dir_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs/youtube_files")
    tp = LocalPorter(dest_dir="./outputs/20230525",
                     interval=10,
                     dir_path=dir_path,
                     clean=True,
                     pack_mode="tar")
    tp.start()

    import time

    time.sleep(60)
    tp.cancel()
