#!/usr/bin/env python3

import os
import random
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pprint import pprint
from secrets import token_urlsafe

from requests import Session


local = threading.local()


def build_targets(targets: dict) -> list:

    out = []

    for f, w in targets.items():
        out += ([f] * w)

    return out


def post(url: str, data: dict, headers: dict):
    try:
        session = local.session
    except AttributeError:
        session = Session()
        local.session = session

    try:
        with session.post(url,
                        json=data,
                        headers=headers) as response:

            if not response.ok:  # some HTTP-level error
                code = response.status_code
                print(code, response.text)
                return str(code)

            return None
    
    except Exception as e:
        t = type(e)
        return f'{t.__module__}.{t.__name__}'


def hit(base_url, targets, headers, test_id):
    func = random.choice(targets)
    url, data = func()
    
    start = time.time()
    error = post(f'{base_url}{url}?testid={test_id}&random={token_urlsafe(7)}', data, headers)
    end = time.time()

    return url, error, end * 1000 - start * 1000


def run(base_url, targets, test_sec, headers=None, test_id=None, exec_type='p'):

    targets = build_targets(targets)

    csv_out = open('results.csv', 'wt')
    csv_out.write('time,completed,avg_resp_time\n')

    futures = set()

    response_time_sum = 0
    max_workers = os.cpu_count() * 2 + 1

    stats = defaultdict(int)
    fails = defaultdict(int)
    
    exec_class = dict(t=ThreadPoolExecutor, p=ProcessPoolExecutor)[exec_type]

    with exec_class(max_workers=max_workers) as executor:

        start_time = time.time()
        report_time = start_time
        requests_completed = 0
        requests_completed_period = 0
        response_time_sum_period = 0

        # requests_completed_report = 0
        requests_submitted = 0

        while time.time() - start_time <= test_sec:
            future = executor.submit(hit, base_url, targets, headers, test_id)
            requests_submitted += 1
            futures.add(future)

            while len(futures) >= max_workers * max(requests_completed_period/max_workers, 10):
                done = False
                for f in list(futures):
                    if f.done():
                        done = True
                        req_endpoint, error, req_time = f.result()
                        stats[req_endpoint] += 1
                        if error:
                            fails[error] += 1
                        
                        response_time_sum += req_time
                        response_time_sum_period += req_time
                        futures.remove(f)
                        requests_completed += 1
                        requests_completed_period += 1

                if not done:
                    time.sleep(0.1)

            if time.time() - report_time > 1:
                now = time.time()
                elapsed = now - start_time
                print(f'[Totals] elapsed: {elapsed:.3f} sec, completed: {requests_completed:,}, '
                      f'submitted: {requests_submitted:,}, pending: {len(futures)}, '
                      f'rps: {requests_completed/elapsed:.1f}, '
                      f'avg resp time: {response_time_sum/requests_completed:.0f} msec')
                print(f'[Period] completed: {requests_completed_period:,}, '
                      f'avg resp time: {response_time_sum_period/requests_completed_period:.0f} msec')
                pprint(dict(stats))
                csv_out.write(f'{elapsed:.3f},{requests_completed_period},{response_time_sum_period/requests_completed_period:.0f}\n')
                csv_out.flush()

                report_time = now
                requests_completed_period = 0
                response_time_sum_period = 0

    for f in futures:
        req_endpoint, error, req_time = f.result()
        stats[req_endpoint] += 1
        if error:
            fails[error] += 1

        response_time_sum += req_time
        requests_completed += 1

    print(f'{requests_completed:,} reqs in {(now - start_time)} sec, '
          f'{response_time_sum / requests_completed:.0f} msec avg response time')
    print(f'{max_workers} workers used')

    print('Counts:')
    pprint(dict(stats))

    print('Errors:')
    pprint(dict(fails))