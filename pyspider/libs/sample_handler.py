#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on __DATE__
# Project: __PROJECT_NAME__

from pyspider.libs.base_handler import *


class Handler(BaseHandler):

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('__START_URL__', callback=self.index_page)

    def index_page(self, response):
        for each in response.doc('a').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    def detail_page(self, response):
        return {
            "body": response.guess_body(), # you may use e.g. div="div.body"
            "title": response.guess_title(), # may be left empty in exceptional cases
            "published": "__STRING_CONTAINING_DATE__",
            "name": "__INSTITUTION_NAME__",
            "short": "__SHORT_INSTITUTION_NAME__",
            "jurisdiction": "US", # country code ISO 3166 alpha-2: US, SK, UN, ...
            "state": "", # can be left empty
            "municipality": "", # can be left empty
            "importance": "LOW", # LOW*|MEDIUM|HIGH
            "document_type": "UNKNOWN", # UNKNOWN*|NEWS|PRESS_RELEASE|COMMENT|ALERT|LITIGATION|OTHER
            "source_type": "UNDEFINED", # UNDEFINED*|REGULATORY_ENFORCEMENT|LAW_ENFORCEMENT|JUDICIARY_COURT_RECORDS|NONGOV_ORG
            "lang": "en",
        }
