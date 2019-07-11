#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

def httpGetWithHeaders(url, headers):
    if url != '' or url is not None or headers is not None:
        response = requests.get(url, headers=headers)
        return response
    else:
        print("WARNING - Please fill in all required params before request get.")

    return None

def httpPostFileWithHeaders(url, payload, files, headers):
    if url != '' or url is not None or headers is not None or payload is not None:
        response = requests.post(url, data=payload, files=files, headers=headers)
        return response
    else:
        print("WARNING - Please fill in all required params before request post.")

    return None
