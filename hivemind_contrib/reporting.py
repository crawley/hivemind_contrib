import os
import sys
import csv
import datetime
import requests
from fabric.api import task
import collections
from prettytable import PrettyTable

import hivemind_contrib.keystone as hm_keystone
import hivemind_contrib.nova as hm_nova

from hivemind.decorators import verbose


def csv_output(headings, rows, filename=None):
    if filename is None:
        fp = sys.stdout
    else:
        fp = open(filename, 'wb')
    csv_output = csv.writer(fp, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
    csv_output.writerow(headings)
    for row in rows:
        csv_output.writerow(map(lambda x: str(x).encode('utf-8'), row))
    if filename is not None:
        fp.close()


def pretty_output(headings, rows, filename=None):
    if filename is None:
        fp = sys.stdout
    else:
        fp = open(filename, 'wb')
    pp = PrettyTable(headings)
    for r in rows:
        pp.add_row(r)
    print >> fp, str(pp)


@task
@verbose
def allocation_homes(csv=False, filename=None):
    """Get the allocation_homes for all projects. If this
metadata field is not set in keystone (see keystone hivemind
commands), the value reported is the email domains for all
tenant managers belonging to this project.
    """
    keystone = hm_keystone.client_session(version=3)
    all_users = map(lambda x: x.to_dict(), keystone.users.list())
    email_dict = {x['id']: x['email'].split("@")[-1] for x in all_users
                  if 'email' in x and x['email'] is not None}
    projects = keystone.projects.list()
    managers = collections.defaultdict(list)
    for user_role in keystone.role_assignments.list(role=14):
        if 'project' in user_role.scope:
            managers[user_role.scope['project']['id']].append(
                user_role.user['id'])
    headings = ["Tenant ID", "Allocation Home(s)"]
    records = []
    for proj in projects:
        if "allocation_home" in proj.to_dict():
            records.append([proj.id, proj.allocation_home])
        else:
            if len(managers[proj.id]) == 0:
                continue
            institutions = set()
            for tm in managers[proj.id]:
                if tm in email_dict:
                    institutions.add(email_dict[tm])
            records.append([proj.id, ",".join(institutions)])
    if csv:
        csv_output(headings, records, filename=filename)
    else:
        pretty_output(headings, records, filename=filename)


@task
@verbose
def allocation_managers(csv=False, filename=None):
    """Get the allocation manager emails for all projects.
    """
    keystone = hm_keystone.client_session(version=3)
    all_users = map(lambda x: x.to_dict(), keystone.users.list())
    email_dict = {x['id']: x['email'] for x in all_users
                  if 'email' in x and x['email'] is not None}
    projects = keystone.projects.list()
    managers = collections.defaultdict(list)
    for user_role in keystone.role_assignments.list(role=14):
        if 'project' in user_role.scope:
            managers[user_role.scope['project']['id']].append(
                user_role.user['id'])
    headings = ["Tenant ID", "Manager email(s)"]
    records = []
    for proj in projects:
        if len(managers[proj.id]) == 0:
            continue
        emails = set()
        for tm in managers[proj.id]:
            if tm in email_dict:
                emails.add(email_dict[tm])
        records.append([proj.id, ",".join(emails)])
    if csv:
        csv_output(headings, records, filename=filename)
    else:
        pretty_output(headings, records, filename=filename)


@task
@verbose
def get_instance_usage_csv(start_date=None, end_date=None, filename=None):
    """Get instance usage statistics for all projects.
Date strings should be ISO 8601 to minute precision
without timezone information.

    """
    assert start_date and end_date
    start = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
    keystone = hm_keystone.client()
    nova = hm_nova.client()

    tenants = {x.id: x for x in keystone.tenants.list()}
    headings = ["Tenant ID", "Tenant Name", "Instance count",
                "Instance hours", "vCPU hours", "Memory Hours (MB)",
                "Disk hours (GB)"]
    usage = map(lambda u: [
                u.tenant_id,
                tenants[u.tenant_id].name if u.tenant_id in tenants else None,
                len(u.server_usages),
                u.total_hours,
                u.total_vcpus_usage,
                u.total_memory_mb_usage,
                u.total_local_gb_usage],
                nova.usage.list(start, end, detailed=True))
    csv_output(headings, usage, filename=filename)


class NectarApiSession(requests.Session):
    """Class to encapsulate the rest api endpoint with a requests session.

    """
    def __init__(self, api_url=None, api_username=None,
                 api_password=None, *args, **kwargs):
        username = os.environ.get('NECTAR_ALLOCATIONS_USERNAME', api_username)
        password = os.environ.get('NECTAR_ALLOCATIONS_PASSWORD', api_password)
        self.api_url = os.environ.get('NECTAR_ALLOCATIONS_URL', api_url)
        assert username and password and self.api_url
        requests.Session.__init__(self, *args, **kwargs)
        self.auth = (username, password)

    def _api_get(self, rel_url, *args, **kwargs):
        return self.get("%s%s" % (self.api_url, rel_url), *args, **kwargs)

    def get_allocations(self):
        req = self._api_get('/api/allocations')
        req.raise_for_status()
        return req.json()

    def get_quotas(self):
        req = self._api_get('/api/quotas')
        req.raise_for_status()
        return req.json()


@task
@verbose
def get_general_allocations_information(filename=None):
    """Get standard allocations information and global quotas for all projects.

    """
    api_endpoint = NectarApiSession()
    allocations = api_endpoint.get_allocations()
    fields_to_report = [
        ("Tenant ID", lambda x: x['tenant_uuid']),
        ("Tenant Name", lambda x: x['tenant_name']),
        ("Project Name", lambda x: x['project_name']),
        ("Allocation Home",
            lambda x: x['allocation_home'] if 'allocation_home' in x
            and x['allocation_home'] is not None else ""),
        ("Status", lambda x: x['status']),
        ("Modified time", lambda x: x['modified_time']),
        ("Instances", lambda x: x['instance_quota']),
        ("vCPU quota", lambda x: x['core_quota']),
        ("RAM quota", lambda x: x['ram_quota']),
        ("FOR 1", lambda x: x['field_of_research_1']),
        ("FOR 1 weighting (%)", lambda x: x['for_percentage_1']),
        ("FOR 2", lambda x: x['field_of_research_2']),
        ("FOR 2 weighting (%)", lambda x: x['for_percentage_2']),
        ("FOR 3", lambda x: x['field_of_research_3']),
        ("FOR 3 weighting (%)", lambda x: x['for_percentage_3']),
        ]

    csv_output(map(lambda x: x[0], fields_to_report),
               map(lambda alloc: map(
                   lambda y: y[1](alloc),
                   fields_to_report),
                   allocations),
               filename=filename)


@task
@verbose
def get_local_allocations_information(filename=None, availability_zone=None):
    """Get local quota information for all projects.

    """
    api_endpoint = NectarApiSession()
    allocations = {x['id']: x for x in api_endpoint.get_allocations()}
    quotas = {}
    quota_fields = []
    for q in api_endpoint.get_quotas():
        if q['zone'] != availability_zone and availability_zone is not None:
            continue
        if q['allocation'] not in quotas:
            quotas[q['allocation']] = collections.defaultdict(int)
        qf = "%(zone)s-%(resource)s (%(units)s)" % q
        quotas[q['allocation']][qf] = "%(quota)s" % q
        if qf not in quota_fields:
            quota_fields.append(qf)

    fields_to_report = ["Tenant ID", "Tenant Name"]
    fields_to_report.extend(quota_fields)

    data_to_report = []
    for a_id in quotas:
        alloc = allocations[a_id]
        row = [alloc['tenant_uuid'],
               alloc['project_name'] if alloc['tenant_name'] is None
               else alloc['tenant_name']]
        row.extend(map(lambda x: quotas[a_id][x], quota_fields))
        data_to_report.append(row)
    csv_output(fields_to_report, data_to_report, filename=filename)
