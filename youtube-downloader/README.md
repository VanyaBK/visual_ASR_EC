# ytb-downloader

This tool is for downloading videos/audio/subtitle from YouTube with [YouTubeDL](https://github.com/ytdl-org/youtube-dl). 

## Scripts

### `search.sh`

This script is for automatically search contents based on ***keyword search*** from YouTube videos. 

#### Arguments

* `-output`: Output directory.
* `-keyword`: Keyword
* `-size`: ideal number of video required
* `-format`: format of saving results, choices:
  * `origin`: save all the metadata of videos
  * `only_id`: only save the video id 
  * `url`: save the video url and video id 
  * `url_title`: save the video url, video title and video id

A sample call of this script is as below: 
```bash
bash search.sh \
  -output <OUTPUT_PATH> \
  -keyword "TikTok" \
  -size 20 \
  -format "url_title"
```


### `download.sh`

This script is for automatically search and download contents based on ***keyword search*** from YouTube videos. 

#### Arguments

* `-query`: HDFS/TOS file for list of query keywords, with one keyword per line. 
* `-output`: Output directory.
* `--must`: List of subtitle languages that are **mandatory** to be included. 
* `--choice`: List of subtitle languages that are **optional** to be included. 
* `--page`: Number of browsed pages (about 50 result per page). 
* `--parallel`: Number of parallel threads. 

A sample call of this script is as below: 
```bash
bash download.sh \
  -query <QUERY_FILE_PATH> \
  -output <OUTPUT_PATH> \
  --must en \
  --page 50 \
  --parallel 32 \
  --skip-download True
```


### `download_urls.sh`

This script is for automatically download contents based on ***URLS/video_ids*** from YouTube. 

#### Arguments

* `-url_file`: File for list of YouTube urls, one video per line.
  * In the format of **"https://www.youtube.com/watch?v={vid} {vid}"**
* `-output`: Output directory.
* `-format`: Format of downloaded contents, available choices: **mp4**, **wav**, **bestvideo**, **worstaudio**, **best[ext=mp4][filesize<30M]** etc. 
  * Refer to [youtubedl GitHub](https://github.com/ytdl-org/youtube-dl#format-selection)
* `-num_workers`: Number of parallel threads. 
* `-interval`: Time interval of porter to synchronize local files to output directory, in seconds.
* `-download_subtitle`: Whether to download the user-uploaded subtitles together with content, `store_true`.
* `-download_info`: Whether to download the information json file together with content, `store_true`.
* `-pack_mode`: Mode of packing when saving files. Selected from `raw`, `tar`, `folder`.
* `--do-not-sync'`: Do not sync, must specify a local output directory.

A sample call of this script is as below: 
```bash
bash download_urls.sh \
  -url_file <QUERY_FILE_PATH> \
  -output <OUTPUT_PATH> \
  -format worstaudio \
  -num_workers 90 \
  -download_subtitle \
  -pack_mode raw
```



