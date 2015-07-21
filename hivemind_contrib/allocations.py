import os
import sys
import datetime
import requests
from fabric.api import task
import collections

import hivemind_contrib.keystone as hm_keystone
import hivemind_contrib.nova as hm_nova

from hivemind.decorators import verbose
from hivemind_contrib.reporting import csv_output, pretty_output, \
    NectarApiSession

def _get_current_allocations():    
    allocations_api = NectarApiSession()
    allocations = allocations_api.get_allocations(); 
    tenant_allocations = {}
    for alloc in allocations:
        if alloc['status'] != 'A' and alloc['status'] != 'X':
            continue
        uuid = alloc['tenant_uuid']
        if uuid == None or uuid == '':
            continue
        if uuid in tenant_allocations:
            old_alloc = tenant_allocations[uuid]
            if alloc['modified_time'] > old_alloc['modified_time']:
                tenant_allocations[uuid] = alloc
        else:
            tenant_allocations[uuid] = alloc
    return tenant_allocations


def _get_flavor_map(nova_api):
    flavors = {}
    for flavor in nova_api.flavors.list(is_public=True):
        flavors[flavor.id] = flavor
    for flavor in nova_api.flavors.list(is_public=False):
        flavors[flavor.id] = flavor
    return flavors


def _get_usage(nova_api, flavors, uuid):
    instances = nova_api.servers.list(search_opts={'project_id': uuid,
                                                   'tenant_id': uuid,
                                                   'all_tenants': 1})
    if len(instances) == 0:
        return {'instances': 0, 'ram': 0, 'vcpus': 0}
    instance_flavors = map(lambda x: flavors[x.flavor['id']], instances);
    vcpus = 0;
    ram = 0;
    for flavor in instance_flavors:
        vcpus += flavor.vcpus
        ram += flavor.ram
    return {'instances': len(instances), 'ram': ram, 'vcpus': vcpus}

@task
@verbose
def crosscheck_usage(filename=None):
    """Cross-check that allocation and instantaneous usage information 
    for all tenants
    """

    allocations = _get_current_allocations()
    nova_api = hm_nova.client()
    flavors = _get_flavor_map(nova_api)
    missing = []
    mismatches = []
    for uuid in allocations.keys():
        alloc = allocations[uuid]
        usage = _get_usage(nova_api, flavors, uuid)
        if (usage['instances'] > alloc['instance_quota']
            or usage['vcpus'] > alloc['core_quota'] 
            or usage['ram'] > alloc['ram_quota'] * 1024):
            alloc['nova_usage'] = usage
            mismatches.append(alloc)

    print '{0} allocations, {1} missing tenants, {2} usage mismatches'.format(
        len(allocations), len(missing), len(mismatches))
    
    fields_to_report = [
        ("Tenant ID", lambda x: x['tenant_uuid']),
        ("Tenant Name", lambda x: x['tenant_name']),
        ("Modified time", lambda x: x['modified_time']),
        ("Instances", lambda x: x['instance_quota']),
        ("Nova instances", lambda x: x['nova_usage']['instances']),
        ("vCPU quota", lambda x: x['core_quota']),
        ("Nova vCPU usage", lambda x: x['nova_usage']['vcpus']),
        ("RAM quota", lambda x: x['ram_quota'] * 1024),
        ("Nova RAM usage", lambda x: x['nova_usage']['ram'])
        ]
    csv_output(map(lambda x: x[0], fields_to_report),
               map(lambda alloc: map(
                   lambda y: y[1](alloc),
                   fields_to_report),
                   mismatches),
               filename=filename)


@task
@verbose
def crosscheck_quotas(filename=None):
    """Cross-check allocation and quota information for all tenants
    """

    allocations = _get_current_allocations()
    nova_api = hm_nova.client()
    missing = []
    mismatches = []
    for uuid in allocations.keys():
        alloc = allocations[uuid]
        try:
            quotas = nova_api.quotas.get(uuid)
        except:
            missing.append(alloc)
            continue
        if (quotas.instances != alloc['instance_quota'] \
            or quotas.ram != alloc['ram_quota'] * 1024 \
            or quotas.cores != alloc['core_quota']):
            alloc['nova_quotas'] = quotas
            mismatches.append(alloc)
    print '{0} allocations, {1} missing tenants, {2} quota mismatches'.format(
        len(allocations), len(missing), len(mismatches))
    
    fields_to_report = [
        ("Tenant ID", lambda x: x['tenant_uuid']),
        ("Tenant Name", lambda x: x['tenant_name']),
        ("Modified time", lambda x: x['modified_time']),
        ("Instances", lambda x: x['instance_quota']),
        ("Nova instances", lambda x: x['nova_quotas'].instances),
        ("vCPU quota", lambda x: x['core_quota']),
        ("Nova vCPU quota", lambda x: x['nova_quotas'].cores),
        ("RAM quota", lambda x: x['ram_quota'] * 1024),
        ("Nova RAM quota", lambda x: x['nova_quotas'].ram)
        ]
    csv_output(map(lambda x: x[0], fields_to_report),
               map(lambda alloc: map(
                   lambda y: y[1](alloc),
                   fields_to_report),
                   mismatches),
               filename=filename)


@task
@verbose
def compare_quotas(name_or_id=None):
    """Compare the allocation and quota information for a tenant
    """
    if name_or_id == None:
        print 'A tenant name or id is required'
        return
        
    keystone_api = hm_keystone.client_session(version=3)
    try:
        tenant = hm_keystone.get_tenant(keystone_api, name_or_id)
    except:
        print 'Tenant {0} not found in keystone'.format(name_or_id)
        return
        
    nova_api = hm_nova.client()
    quotas = nova_api.quotas.get(tenant.id)
    print 'nova quotas: instances {0}, cores {1}, ram {2}'.format(
        quotas.instances, quotas.cores, quotas.ram / 1024)
    usage = _get_usage(nova_api, _get_flavor_map(nova_api), tenant.id)
    print 'nova isage: instances {0}, cores {1}, ram {2}'.format(
        usage['instances'], usage['vcpus'], usage['ram'] / 1024)

    allocations_api = NectarApiSession()
    allocations = allocations_api.get_allocations(); 
    tenant_allocations = filter(lambda x: x['tenant_uuid'] == tenant.id and \
                                (x['status'] == 'A' or x['status'] == 'X'),
                                allocations)
    if len(tenant_allocations) == 0:
        print 'No approved allocation records for tenant {0} / {1}'.format(
            tenant.id, tenant.name)
        return

    tenant_allocations.sort(key=lambda alloc: alloc['modified_time'])
    current_allocation = tenant_allocations[-1]
    
    format = '{0} mismatch: allocated {1}, nova {2}, used {3}'
    if current_allocation['instance_quota'] != quotas.instances:
        print format.format('Instance quota',
                            current_allocation['instance_quota'], 
                            quotas.instances,
                            usage['instances'])
    if current_allocation['core_quota'] != quotas.cores:
        print format.format('VCPU quota',
                            current_allocation['core_quota'], 
                            quotas.cores,
                            usage['vcpus'])
    if current_allocation['ram_quota'] * 1024 != quotas.ram:
        print format.format('RAM quota',
                            current_allocation['ram_quota'] * 1024, 
                            quotas.ram,
                            usage['ram'])

