# -*- coding: utf-8 -*-
"""
Capture voice/video based on subtitle timestamp
"""
import re


def process_subtitle(subtitle_file):
    content = subtitle_file.read().split('\n\n')[1:-1]
    pat = re.compile(r"[\s\uFFFD]+")
    subtitle_list = []

    def time2milisec(time_str):
        time_seps = re.split(r'\:|\.', time_str)
        return int(time_seps[3]) + 1000 * int(time_seps[2]) + 60000 * int(time_seps[1]) + 3600000 * int(time_seps[0])

    for block in content:
        lines = block.split('\n')
        timeline = lines[0].split(' --> ')
        timeline[1] = timeline[1].split(' ')[0].strip()
        start_time, end_time = time2milisec(timeline[0]), time2milisec(timeline[1])
        subtitle_text = ' '.join(lines[1:]).strip()
        subtitle_text = re.sub(pat, ' ', subtitle_text)
        # if subtitle_list and (subtitle_text[0].islower() or (start_time - subtitle_list[-1][1] < 500)):
        if len(subtitle_text) < 5:
            continue
        if subtitle_list and subtitle_text[0].islower():
            (last_start, last_end, last_subtitle_text) = subtitle_list.pop()
            subtitle_list.append((last_start, end_time, ' '.join([last_subtitle_text, subtitle_text])))
        else:
            subtitle_list.append((start_time, end_time, subtitle_text))

    return subtitle_list
