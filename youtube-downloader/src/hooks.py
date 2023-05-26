# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
from pathlib import Path


def tag_success(d):
    if d['status'] == 'finished':
        Path(os.path.join(os.path.dirname(d["filename"]), '_SUCCESS')).touch()
