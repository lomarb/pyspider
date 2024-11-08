#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-19 16:23:55

from __future__ import unicode_literals

import json
from flask import render_template, request, json
from flask import Response
from .app import app
from pyspider.libs import result_dump


@app.route('/results', methods=['POST', 'GET'])
def result():
    resultdb = app.config['resultdb']
    project = request.values.get('project')
    offset = int(request.values.get('offset', 0))
    limit = int(request.values.get('limit', 20))
    fields = json.loads(request.values.get('fields', '{}')) or None
    filter = json.loads(request.values.get('filter', '{}')) or None

    count = resultdb.count(project, filter)
    results = list(resultdb.select(project, fields=fields, offset=offset, limit=limit, filter=filter))

    return render_template(
        "result.html", count=count, results=results,
        result_formater=result_dump.result_formater,
        project=project, fields=fields, offset=offset, limit=limit, filter=filter, json=json
    )


@app.route('/results/dump/<project>.<_format>', methods=['POST', 'GET'])
def dump_result(project, _format):
    resultdb = app.config['resultdb']
    # force update project list
    resultdb.get(project, 'any')
    if project not in resultdb.projects:
        return "no such project.", 404

    offset = int(request.values.get('offset', 0))
    limit = int(request.values.get('limit', 0))
    fields = json.loads(request.values.get('fields', '{}')) or None
    filter = json.loads(request.values.get('filter', '{}')) or None
    
    results = resultdb.select(project, fields=fields, offset=offset, limit=limit, filter=filter)

    if _format == 'json':
        valid = request.values.get('style', 'rows') == 'full'
        return Response(result_dump.dump_as_json(results, valid),
                        mimetype='application/json')
    elif _format == 'txt':
        return Response(result_dump.dump_as_txt(results),
                        mimetype='text/plain')
    elif _format == 'csv':
        return Response(result_dump.dump_as_csv(results),
                        mimetype='text/csv')
