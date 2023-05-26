# -*- coding: utf-8 -*-
"""
Script for downloading YouTube contents by urls
"""

import os
from downloader import download_specific
from portor import HdfsPorter, LocalPorter, StatisticPorter
from hooks import tag_success
from multiprocessing import Pool
import argparse


def dl_urls(url_dict):
    """
    Download contents from urls, specific configs on requested formats
    :return:
    """
    # url_dict = {i[0]: i[1] for i in news_video_list}

    if args.format == "mp3":
        custom_setting = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }]
        }
    elif args.format == "wav":
        custom_setting = {
            'format': 'worstaudio',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
            }],
        }
    else:
        custom_setting = {
            'format': args.format,
        }
    custom_setting.update({
            'writeinfojson': args.download_info,
            'writesubtitles': args.download_subtitle,
            'outtmpl': 'youtube_files/%(id)s/name.%(ext)s'.replace("youtube_files", args.output),
            'progress_hooks': [tag_success]
        })
    _ = download_specific(url_dict, "name", custom_setting)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Download src videos with given url file.')
    parser.add_argument(
        '--url-file', '-f',
        required=True,
        help='Specify the url file that contains download urls')

    parser.add_argument(
        '--output', '-o',
        required=False,
        help='Specify the url file that contains download urls')

    parser.add_argument(
        '--remote-output', '-ro',
        required=False,
        help="path for saving the results finally"
    )

    parser.add_argument(
        '--format', '-fmt',
        required=False,
        default='mp4',
        help='Specify the download format')

    parser.add_argument(
        '--sync-interval', '-s',
        required=False,
        default=60,
        type=int,
        help='Specify the download format')

    parser.add_argument(
        '--num-workers', '-n',
        required=False,
        default=1,
        type=int,
        help='Specify number of worker threads')

    parser.add_argument(
        '--pack-mode',
        type=str,
        default='tar',
        choices=['tar', 'raw', 'folder'],
    )

    parser.add_argument(
        '--download-subtitle',
        default=False,
        action='store_true',
        help='Whether to download subtitle file')

    parser.add_argument(
        '--download-info',
        default=False,
        action='store_true',
        help='Whether to download video information')

    parser.add_argument(
        '--do-not-sync',
        default=False,
        action='store_true',
        help='Do not use Porter to sync'
    )

    args = parser.parse_args()

    local_dict_file = args.url_file

    dir_path = args.output
    remote_output_path = args.remote_output
    if args.do_not_sync or (not remote_output_path) or (remote_output_path == dir_path):
        hp = StatisticPorter(interval=args.sync_interval,
                             clean=True,
                             dir_path=dir_path)
    elif remote_output_path.startswith("hdfs://"):
        hp = HdfsPorter(dest_dir=remote_output_path,
                        interval=args.sync_interval,
                        clean=True,
                        dir_path=dir_path,
                        pack_mode=args.pack_mode)
    else:
        hp = LocalPorter(dest_dir=remote_output_path,
                         interval=args.sync_interval,
                         clean=True,
                         dir_path=dir_path,
                         pack_mode=args.pack_mode)

    hp.start()

    thread_pool = Pool(args.num_workers)

    if not os.path.exists(local_dict_file):
        raise Exception(f"Local file {local_dict_file} doesn't exist")

    with open(local_dict_file) as f:
        news_video_list = [line.strip().split() for line in f]
        record_list = [{i[0]: i[1]} for i in news_video_list]
    for record in record_list:
        thread_pool.apply_async(dl_urls, (record,))
    thread_pool.close()
    thread_pool.join()

    # time.sleep(10000 * args.sync_interval)

    hp.cancel()
