#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Blueprint
from flask import request
from api.util.logging import Logging as logger


base_v1 = Blueprint("base_v1", __name__)


@base_v1.route('/base', methods=['GET'])
def base_hello():
    logger.info(request.remote_addr, "Hello Base version 1.")
    return "Hello Base version 1"

