# -*- coding: utf-8 -*-
"""
YouTube downloader
By default, it integrates YoutubeDL and YouTubeSearcher, and also provides some simple wrapper functions for use alone
YOUTUBE_DL_DEFAULT_SETTING the default YoutubeDL configuration file, which can be modified through set_custom_setting, only a few parameters can be modified at present
download_from_url, download_specific are simple packages of YoutubeDL for downloading some video subtitles directly
search_and_download is the serial function of the search and download process, and it is also the interface that the script directly calls the module
"""
import io
import json
from datetime import datetime
import traceback
import re
import argparse
from copy import deepcopy
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED
import cv2
import requests
from pydub import AudioSegment
from tqdm import tqdm

from yt_dlp import YoutubeDL

from get_contents import process_subtitle
from log_settings import get_logger
from youtube_search import YouTubeSearcher
from portor import LocalPorter, StatisticPorter, HdfsPorter
from hooks import tag_success


logger = get_logger("downloader")
GROUP_NUM = 5
FAIL_RETRY_TIME = 3
# maintain default src-dl settings
YOUTUBE_DL_DEFAULT_SETTING = {
    'quiet': True,
    'no_warnings': False,
    'simulate': False,
    'format': 'bestaudio/worst[ext=mp4]',  # todo
    'outtmpl': 'youtube_files/%(id)s/%(id)s.%(ext)s',
    'progress_hooks': [tag_success],
    'cachedir': "youtube_cache/",
    'ignoreerrors': True,
    'restrictfilenames': True,
    'force_generic_extractor': False,
    'playliststart': 1,
    'playlistend': -1,
    'matchtitle': False,
    'rejecttitle': False,
    'logger': logger,
    'logtostderr': False,
    'writedescription': False,
    'writeinfojson': True,
    'writeannotations': False,
    'writethumbnail': False,
    'write_all_thumbnails': False,
    'writesubtitles': True,  # todo
    'writeautomaticsub': False,
    'nooverwrites': True,
    'allsubtitles': False,
    'listsubtitles': False,
    'subtitlesformat': 'srt/best',
    'subtitleslangs': ['en', 'en-US', 'en-UK', 'zh', 'zh-CN', 'zh-Hans', 'zh-HK', 'zh-Hant', ],
    'keepvideo': False,
    'skip_download': False,
    'nocheckcertificate': True,
    'prefer_insecure': True,
    # 'proxy': 'URL of the proxy server to use',
    # 'socket_timeout': 'Time to wait for unresponsive hosts, in seconds',
    'include_ads': False,
    # 'encoding': 'Use this encoding instead of the system-specified.',
    'socket_timeout': 15,
    'sleep_interval': 1,
    'max_sleep_interval': 5,
    'retries': 3,
    'continuedl': True,
    # The following options determine which downloader is picked:
    # external_downloader: Executable of the external downloader to call.
    #                    None or unset for standard (built-in) downloader.
    # hls_prefer_native: Use the native HLS downloader instead of ffmpeg/avconv
    #                    if True, otherwise use ffmpeg/avconv if False, otherwise
    #                    use downloader suggested by extractor if None.
    #
    # The following parameters are not used by YoutubeDL itself, they are used by
    # the downloader (see youtube_dl/downloader/common.py):
    # nopart, updatetime, buffersize, ratelimit, min_filesize, max_filesize, tests,
    # noresizebuffer, retries, continuedl, noprogress, consoletitle,
    # xattr_set_filesize, external_downloader_args, hls_use_mpegts,
    # http_chunk_size.
    #
    # The following options are used by the post processors:
    # prefer_ffmpeg:     If False, use avconv instead of ffmpeg if both are available,
    #                    otherwise prefer ffmpeg.
    # ffmpeg_location:   Location of the ffmpeg/avconv binary; either the path
    #                    to the binary or its containing directory.
    # postprocessor_args: A list of additional command-line arguments for the
    #                     postprocessor.
    #
    # The following options are used by the Youtube extractor:
    # youtube_include_dash_manifest: If True (default), DASH manifests and related
    #                     data will be downloaded and processed by extractor.
    #                     You can reduce network I/O by disabling it if you don't
    #                     care about DASH.

}

LANG_DICT = {
    "en": ['en', 'en-US', 'en-UK', "en-GB"],
    "zh": ['zh', 'zh-CN', 'zh-Hans', 'zh-TW', 'zh-HK', 'zh-Hant'],
    "fr": ["fr-FR", "fr", "fr-CA"],
    "de": ["de-DE", "de"],
    'ja': ["ja", "jp"],
    'pt': ["pt"],
    'ko': ["ko"],
    'it': ["it"]
}

# =============================tools======================================


# generate urls from video id list
def gen_urls_from_vid(vid_list):
    return ['https://www.youtube.com/watch?v={}'.format(_id) for _id in vid_list]


def is_contain_chinese(s):
    """
    check if the input string contains Chinese characters: \u4e00 - \u9fff
    :return: {bool}
    """
    try:
        for ch in s:
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
        return False
    except Exception as e:
        logger.debug(e)
        return True


def generalization_lang(lang_abbr):
    extract = re.findall(r"(.+?)-.*", lang_abbr)
    return extract[0] if extract else lang_abbr


def is_satisfy(data, must, choice):
    try:
        subs = data['subtitles'].keys()
        logger.debug("subtitles: {}-{}-{}".format(list(subs), must, choice))
        intersection = (set(must).union(set(choice))).intersection(set(subs))
        # subs = list(set([generalization_lang(_) for _ in subs]))
        # if False in [True if lang in subs else False for lang in must]:
        #     return False
        # intersection = [s for s in subs if s in choice]
        if not intersection:
            return False
        return True
    except Exception as e:
        logger.warning("filter_subtitle resolve failed, check if the format has changed -- {}".format(data))
        return False


def download_from_url(client: YoutubeDL, url):
    return client.extract_info(url=url, download=True)


def download_specific(url_dict, place_holder, custom_setting=None):
    for url, name in url_dict.items():
        yd_setting = deepcopy(YOUTUBE_DL_DEFAULT_SETTING)
        if custom_setting:
            for k, v in custom_setting.items():
                if isinstance(v, str):
                    yd_setting[k] = v.replace(place_holder, name)
                else:
                    yd_setting[k] = v
        yd = YoutubeDL(params=yd_setting)
        info = yd.extract_info(url, download=True)


def read_query_from_file(f_path):
    query = set()
    with open(f_path, 'r', encoding='utf-8') as f:
        while True:
            line = f.readline().strip()
            if not line:
                break
            query.add(line)
    return list(query)


# ============================ downloader process ===========================

# split the subtitles and download audios/videos
def get_video_contents(info, parsed_arg, lang_must, lang_choice):
    download_success = False
    vid = info['id']
    output_dir = parsed_arg.output
    try:
        lang_list = list((set(lang_must).union(set(lang_choice))).intersection(set(info['subtitles'])))
        lang = lang_list[0]
        new_info = {'id': vid, 'title': info['title'], 'lang': lang}
        with open(f'{output_dir}/{vid}/{vid}.{lang}.vtt') as subtitle_file:
            subtitle_list = process_subtitle(subtitle_file)
        formats = info['formats']
        best_audio_size, best_audio_fm = 0, None
        for fm in formats:
            if fm['height'] is None and fm['width'] is None and fm['filesize'] > best_audio_size:
                best_audio_size = fm['filesize']
                best_audio_fm = fm
        video_quality_pref = ['480p', '360p', '720p', '1080p', '240p', '144p']
        best_video_fm = None
        for quality in video_quality_pref:
            if best_video_fm is None:
                for fm in formats:
                    if fm['format_note'] == quality:
                        best_video_fm = fm
            else:
                break

        contents = []
        audio_content = requests.get(best_audio_fm['url'], headers=best_audio_fm['http_headers'], stream=True).content
        audio_content = AudioSegment.from_file(io.BytesIO(audio_content), best_audio_fm['ext'])
        if best_video_fm is not None:
            vc = cv2.VideoCapture()
            vc.open(best_video_fm['url'])
        else:
            return False

        all_captured = True
        for sub in subtitle_list:
            if not all_captured:
                break
            start_time, end_time = sub[0], sub[1]
            if len(sub[2].split(' ')) <= 3:
                continue
            segment = {
                'start_timestamp': start_time,
                'end_timestamp': end_time,
                'subtitle_text': sub[2],
                'audio_segment': f"{vid}.audio.{start_time:08d}-{end_time:08d}.wav",
                'video_captures': []
            }
            audio_chunk = audio_content[start_time:end_time]
            audio_chunk.export(f"youtubedl/youtube_files/{vid}/{vid}.audio.{start_time:08d}-{end_time:08d}.wav")

            vc.set(cv2.CAP_PROP_POS_MSEC, start_time)
            rval, frame = vc.read()
            if rval:
                cv2.imwrite(f"youtubedl/youtube_files/{vid}/{vid}.video.{start_time:08d}.png", frame)
                segment['video_captures'].append(f"{vid}.video.{start_time:08d}.png")
            else:
                all_captured = False
                break
            mid_time = int((start_time + end_time) / 2)
            vc.set(cv2.CAP_PROP_POS_MSEC, mid_time)
            rval, frame = vc.read()
            if rval:
                cv2.imwrite(f"youtubedl/youtube_files/{vid}/{vid}.video.{mid_time:08d}.png", frame)
                segment['video_captures'].append(f"{vid}.video.{mid_time:08d}.png")
            else:
                all_captured = False
                break
            contents.append(segment)

        if not all_captured:
            logger.error(f"download {info['id']} video captures fail!")
            return False

        new_info['contents'] = contents
        with open(f'{output_dir}/{vid}/{vid}.contents.json', 'w') as write_file:
            json.dump(new_info, write_file)
        open(f'{output_dir}/{vid}/_SUCCESS', 'a').close()
        download_success = True
    except Exception as e:
        traceback.print_exc()
        logger.error(f"download {info['id']} contents fail!")

    return download_success


# filter video by subtitle language
def filter_subtitle(client, url, lang_must, lang_choice, parsed_arg, download=False):
    download_success = False
    try:
        info = client.extract_info(url, ie_key='Youtube', download=False, process=False)
        if not info:
            raise ConnectionError("extract_info fail {}".format(url))
    except Exception as e:
        traceback.print_exc()
        return True, None, False
    satisfy = is_satisfy(info, lang_must, lang_choice)
    if satisfy:
        # logger.debug("{} satisfy: {}".format(url, info))
        info.update({"_type": "video"})
        if download:
            try:
                client.process_video_result(info, download=True)
                logger.info(f"downloading contents of {url}...")
                download_success = get_video_contents(info, parsed_arg, lang_must, lang_choice)
            except Exception as e:
                traceback.print_exc()
                logger.error("download {} fail".format(url))
        return satisfy, info, download_success
    else:
        return satisfy, None, download_success


def download_group_with_filter(yd_setting, urls, lang_must, lang_choice, parsed_arg, download=False):
    yd = YoutubeDL(params=yd_setting)
    success = []
    fail = []
    filtered = []
    for url in urls:
        satisfy, info, download_success = filter_subtitle(yd, url, lang_must, lang_choice, parsed_arg, download)
        if satisfy:
            if download_success:
                success.append(url)
            else:
                fail.append(url)
        else:
            filtered.append(url)
    return success, fail, filtered


def search_and_download(searcher: YouTubeSearcher, dl_setting, parsed_arg: Namespace):
    query_words = read_query_from_file(parsed_arg.query_file)
    logger.info("all query words: {}".format(query_words))
    fail_retry_dict = dict()
    executor = ThreadPoolExecutor(parsed_arg.parallel)
    pool = []
    group = list()
    for query in tqdm(query_words, desc='query progress'):
        for item in searcher.search_iter(query_words=query, subtitle_filter=args.subtitles_filter, page_total=args.page):
            if parsed_arg.chinese and not is_contain_chinese(item['title']):
                logger.warning("there are no chinese in title, filtered {}".format(item))
                continue
            if len(group) < GROUP_NUM:
                group.append(item['url'])
                continue
            # logger.info('query-{}-{} items-{}'.format(query, len(group), group))
            must_list, choice_list = [], []
            for m in parsed_arg.must:
                must_list.extend(LANG_DICT.get(m, [m]))
            if parsed_arg.choice:
                for c in parsed_arg.choice:
                    choice_list.extend(LANG_DICT.get(c, [c]))
            must_list = list(set(must_list))
            choice_list = list(set(choice_list))
            future = executor.submit(download_group_with_filter, dl_setting, deepcopy(group),
                                     must_list, choice_list, parsed_arg, True)
            pool.append(future)
            group.clear()
            if len(pool) > parsed_arg.parallel * 2:
                done, not_done = wait(pool, return_when=FIRST_COMPLETED)
                for fu in done:
                    success, fail, filtered = fu.result()
                    logger.info("success:{}, filtered:{}".format(",".join(success), ",".join(filtered)))
                    if fail:
                        logger.warning("fail {}, will be added to next group".format(",".join(fail)))
                    for f_url in fail:
                        if fail_retry_dict.get(f_url, 0) < FAIL_RETRY_TIME:
                            group.append(f_url)
                            if f_url in fail_retry_dict:
                                fail_retry_dict[f_url] += 1
                            else:
                                fail_retry_dict[f_url] = 1
                pool = list(not_done)
    done, not_done = wait(pool, return_when=ALL_COMPLETED)
    for fu in done:
        success, fail, filtered = fu.result()
        logger.info("success:{}, filtered:{}".format(",".join(success), ",".join(filtered)))
        if fail:
            logger.warning("fail {}, will be dropped".format(",".join(fail)))


def parse_download_args():
    def str2bool(v):
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Unsupported value encountered.')

    today = datetime.today().strftime('%Y%m%d')
    parser = argparse.ArgumentParser(description='Youtube Downloader')

    # for search
    parser.add_argument('--query-file', '-q', type=str, default='query.txt', help="file containing searching keywords, one keyword per line")
    parser.add_argument('--subtitles-filter', type=str2bool, default=True, help="only select videos with subtitles")
    parser.add_argument('--chinese', '-c', type=str2bool, default=False, help="only select titles with Chinese")

    # for download
    parser.add_argument("--must", dest="must", nargs='+', help="compulsory list of languages with subtitles")
    parser.add_argument("--choice", dest="choice", nargs='+', help="optional list of languages with subtitles")
    parser.add_argument("--output", '-o', type=str, default=f"outputs/{today}", help="output dir")
    parser.add_argument("--format", '-f', type=str, default="bestaudio/worst[ext=mp4]", help="download format, refer to https://github.com/ytdl-org/youtube-dl#format-selection")
    parser.add_argument("--page", type=int, default=0, help="number of searching pages, default for all pages")
    parser.add_argument("--parallel", type=int, default=1, help="number of threads")
    parser.add_argument('--skip-download', type=str2bool, default=False, help="whether skip video/audio download, default False")
    parser.add_argument('--sync-interval', '-s', required=False, default=60, type=int, help="syncing time interval")
    parser.add_argument('--remote-output', '-ro', required=False, help="path for saving the results finally")
    parser.add_argument('--pack-mode', type=str, default='tar', choices=['tar', 'raw', 'folder'])
    tmp = parser.parse_args()
    return tmp


def set_custom_setting(parsed_arg, setting):
    for i in parsed_arg.must:
        setting["subtitleslangs"] = setting["subtitleslangs"] + list(set(LANG_DICT.get(i, [i])) - set(setting["subtitleslangs"]))
    if parsed_arg.choice:
        for i in parsed_arg.choice:
            setting["subtitleslangs"] = setting["subtitleslangs"] + list(set(LANG_DICT.get(i, [i])) - set(setting["subtitleslangs"]))
    if parsed_arg.output:
        setting['outtmpl'] = setting['outtmpl'].replace("youtube_files", parsed_arg.output)
    if parsed_arg.format:
        setting['format'] = parsed_arg.format
    if parsed_arg.skip_download:
        setting['skip_download'] = parsed_arg.skip_download


if __name__ == '__main__':
    args = parse_download_args()
    settings = deepcopy(YOUTUBE_DL_DEFAULT_SETTING)
    set_custom_setting(args, settings)

    dir_path = args.output
    remote_output_path = args.remote_output
    if (not remote_output_path) or (remote_output_path == dir_path):
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
    yts = YouTubeSearcher()
    search_and_download(yts, settings, args)
    hp.cancel()
