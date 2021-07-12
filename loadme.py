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


class Runner:
    def __init__(self, base_url, targets, test_sec, headers, test_id, exec_type):
        self.base_url = base_url
        self.targets = build_targets(targets)
        self.test_sec = test_sec
        self.headers = headers
        self.test_id = test_id

        self.stats = defaultdict(int)
        self.fails = defaultdict(int)

        self.response_time = 0
        self.requests_completed = 0
        self.requests_submitted = 0

        self.max_workers = os.cpu_count() * 2 + 1

        if exec_type == 'p':
            self.exec_class = ProcessPoolExecutor
        elif exec_type == 't':
            self.exec_class = ThreadPoolExecutor
        else:
            raise ValueError('Invalid exec_type ' + exec_type)

    def handle_result(self, fut):
        req_endpoint, error, req_time = fut.result()
        self.stats[req_endpoint] += 1
        if error:
            self.fails[error] += 1

        self.response_time += req_time
        self.requests_completed += 1

        return req_time

    def run(self):

        csv_out = open('results.csv', 'wt')
        csv_out.write('time,completed,avg_resp_time\n')

        futures = set()

        with self.exec_class(max_workers=self.max_workers) as executor:
            print('Executor:', executor)

            start_time = time.time()
            report_time = start_time
            requests_completed_period = 0
            response_time_sum_period = 0

            while time.time() - start_time <= self.test_sec:
                future = executor.submit(hit, self.base_url, self.targets, self.headers, self.test_id)
                self.requests_submitted += 1
                futures.add(future)

                while len(futures) >= self.max_workers * max(requests_completed_period / self.max_workers, 10):
                    done = False
                    for f in list(futures):
                        if f.done():
                            done = True
                            response_time_sum_period += self.handle_result(f)
                            requests_completed_period += 1

                            futures.remove(f)

                    if not done:
                        time.sleep(0.1)

                if time.time() - report_time > 1:
                    now = time.time()
                    elapsed = now - start_time

                    print(f'[Totals] elapsed: {elapsed:.3f} sec, completed: {self.requests_completed:,}, '
                          f'submitted: {self.requests_submitted:,}, pending: {len(futures)}, '
                          f'rps: {self.requests_completed / elapsed:.1f}, '
                          f'avg resp time: {self.response_time / self.requests_completed:.0f} msec')

                    self.print_stats()

                    print(f'[Period] completed: {requests_completed_period:,}, '
                          f'avg resp time: {response_time_sum_period / requests_completed_period:.0f} msec')

                    csv_out.write(
                        f'{elapsed:.3f},{requests_completed_period},{response_time_sum_period / requests_completed_period:.0f}\n')
                    csv_out.flush()

                    report_time = now
                    requests_completed_period = 0
                    response_time_sum_period = 0

        for f in futures:
            self.handle_result(f)

        print('===== Summary =====')

        print(f'{self.requests_completed:,} reqs in {(now - start_time)} sec, '
              f'{self.response_time / self.requests_completed:.0f} msec avg response time')
        print(f'{self.max_workers} workers used')

        self.print_stats()

    def print_stats(self):
        print('Counts:')
        pprint(dict(self.stats))

        print('Errors:')
        pprint(dict(self.fails))


def run(base_url, targets, test_sec, headers=None, test_id=None, exec_type='p'):
    Runner(base_url, targets, test_sec, headers, test_id, exec_type).run()
