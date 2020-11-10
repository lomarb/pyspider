#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2015-01-17 12:32:17

import os
import re
import glob
import time
import logging
import json

from pyspider.database.base.projectdb import ProjectDB as BaseProjectDB

class ProjectDB(BaseProjectDB):
    """ProjectDB loading scripts from local JSON files."""

    def __init__(self, path):
        self.path = path[0]
        self.projects = {}
        self.load_scripts()

    def load_scripts(self):
        project_names = set(self.projects.keys())
        for filename in glob.glob(os.path.join(self.path, "*.json")):
            name = os.path.basename(filename)[:-5]
            if name in project_names:
                project_names.remove(name)
            updatetime = os.path.getmtime(filename)
            if name not in self.projects or (self.projects[name].get("updatetime", None) and updatetime > self.projects[name]['updatetime']):
                with open(filename) as f:
                    project = json.load(f)
                if not project:
                    continue
                project["updatetime"] = time.time()
                with open(os.path.join(self.path, name + ".py")) as f:
                    project["script"] = f.read() or "\n"
                self.projects[name] = project
        for name in project_names:
            del self.projects[name]

    def drop(self, name):
        if name in self.projects:
            del self.projects[name]
        fn = os.path.join(self.path, name + ".json")
        if os.path.exists(fn):
            os.unlink(fn)
        fn = os.path.join(self.path, name + ".py")
        if os.path.exists(fn):
            os.unlink(fn)

    def insert(self, name, obj={}):
        if name in self.projects:
            logging.warning(f"Project already exists: {name}")
            return None
        self.projects[name] = obj
        self.projects[name]["updatetime"] = time.time()
        self.projects[name]["name"] = name
        self.projects[name]["group"] = obj.get("group", "generic")
        with open(os.path.join(self.path, name + ".py"), "w") as outfile:
            outfile.write(obj["script"] or "\n")
        metadata = {k: v for k, v in self.projects[name].items() if k != "script"}
        with open(os.path.join(self.path, name + ".json"), "w") as outfile:
            outfile.write(json.dumps(metadata, indent=4, ensure_ascii=False))
        return self.projects[name]

    def update(self, name, obj={}, **kwargs):
        obj = dict(obj)
        obj.update(kwargs)
        obj["updatetime"] = time.time()
        if name not in self.projects:
            logging.warning(f"Cannot update: project {name} does not exist.")
            return None
        self.projects[name].update(obj)
        metadata = {k: v for k, v in self.projects[name].items() if k != "script"}
        with open(os.path.join(self.path, name + ".json"), "w") as outfile:
            outfile.write(json.dumps(metadata))
        with open(os.path.join(self.path, name + ".py"), "w") as outfile:
            outfile.write(self.projects[name]["script"] or "\n")
        return self.projects[name]

    def get_all(self, fields=None):
        for projectname in self.projects:
            yield self.get(projectname, fields)

    def get(self, name, fields=None):
        if name not in self.projects:
            return None
        project = self.projects[name]
        result = {}
        for f in fields or project:
            if f in project:
                result[f] = project[f]
            else:
                result[f] = None
        return result

    def check_update(self, timestamp, fields=None):
        self.load_scripts()
        for projectname in self.projects:
            if self.projects[projectname]['updatetime'] > timestamp:
                yield self.get(projectname, fields)

    def verify_project_name(self, name):
        return len(name) <= 64
