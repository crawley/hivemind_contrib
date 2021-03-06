import os
import sys
import time
import urlparse

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from fabric.api import task
from novaclient import client as nova_client
from prettytable import PrettyTable
from hivemind.decorators import verbose, only_for, configurable
from hivemind.operations import run
from hivemind.util import current_host

from hivemind_contrib.swift import client as swift_client

DEFAULT_AZ = 'melbourne-qh2'
DEFAULT_SECURITY_GROUPS = 'default,openstack-node,puppet-client'

FILE_TYPES = {
    'cloud-config': '#cloud-config',
    'x-shellscript': '#!',
}


@configurable('nectar.openstack.client')
def client(url=None, username=None, password=None, tenant=None):
    url = os.environ.get('OS_AUTH_URL', url)
    username = os.environ.get('OS_USERNAME', username)
    password = os.environ.get('OS_PASSWORD', password)
    tenant = os.environ.get('OS_TENANT_NAME', tenant)
    assert url and username and password and tenant
    return nova_client.Client('2',
                              username=username, api_key=password,
                              project_id=tenant, auth_url=url)


def list_services():
    output = run("nova-manage service list 2>/dev/null")
    services = []
    header = None
    for line in output.split("\n"):
        if not header:
            header = [l.lower() for l in line.split()]
            continue
        services.append(dict(zip(header, line.split())))
    return services


def host_services(host=None):
    if not host:
        host = current_host()
    return [service for service in list_services()
            if service["host"] == host]


@only_for("nova-node", "nova-controller")
def disable_host_services(host=None):
    if not host:
        host = current_host()
    for service in host_services(host):
        run("nova-manage service disable --host %s --service %s" %
            (service["host"], service["binary"]))


@only_for("nova-node", "nova-controller")
def enable_host_services(host=None):
    if not host:
        host = current_host()
    for service in host_services(host):
        run("nova-manage service enable --host %s --service %s" %
            (service["host"], service["binary"]))


def get_flavor_id(client, flavor_name):
    flavors = client.flavors.list()
    for flavor in flavors:
        if flavor.name == flavor_name:
            return flavor.id
    raise Exception("Can't find flavor %s" % flavor_name)


def wait_for(func, error_message):
    for i in xrange(60):
        ret = func()
        if ret:
            return ret
        time.sleep(1)
    raise Exception(error_message)


def server_address(client, id):
    server = client.servers.get(id)
    if not server.addresses:
        return None
    for name, addresses in server.addresses.items():
        for address in addresses:
            if address.get('addr'):
                return address['addr']


def combine_files(file_contents):
    combined_message = MIMEMultipart()
    for i, contents in enumerate(file_contents):
        for content_type, start in FILE_TYPES.items():
            if contents.startswith(start):
                break
        else:
            raise Exception("Can't find handler for '%s'" %
                            contents.split('\n', 1)[0])

        sub_message = MIMEText(contents, content_type,
                               sys.getdefaultencoding())
        sub_message.add_header('Content-Disposition',
                               'attachment; filename="file-%s"' % i)
        combined_message.attach(sub_message)
    return combined_message


def file_contents(filenames):
    for filename in filenames:
        url = urlparse.urlsplit(filename)
        if url.scheme == 'swift':
            resp = swift_client().get_object(url.netloc, url.path.strip('/'))
            yield resp[1]
        elif not url.scheme:
            with open(url.path) as fh:
                yield fh.read()
        else:
            raise ValueError('Unrecognised url scheme %s' % url.scheme)


@task
@verbose
def boot(name, key_name=None, image_id=None, flavor='m1.small',
         security_groups=DEFAULT_SECURITY_GROUPS,
         networks=[], userdata=[], availability_zone=DEFAULT_AZ):
    """Boot a new server.

       :param str name: The name you want to give the VM.
       :param str keyname: Key name of keypair that should be used.
       :param str flavor: Name or ID of flavor,
       :param str security_groups: Comma separated list of security
         group names.
       :param list userdata: User data file to pass to be exposed by
         the metadata server.
       :param list networks: A list of networks that the VM should
         connect to. net-id: attach NIC to network with this UUID
         (required if no port-id), v4-fixed-ip: IPv4 fixed address
         for NIC (optional), port-id: attach NIC to port with this
         UUID (required if no net-id).
         (e.g. net-id=<net-uuid>;v4-fixed-ip=<ip-addr>;port-id=<port-uuid>,...)
       :param str availability_zone: The availability zone for
         instance placement.
       :param choices ubuntu: The version of ubuntu you would like to use.

    """
    nova = client()

    flavor_id = get_flavor_id(nova, flavor)

    nics = []
    for net in networks:
        nics.append({})
        for option in net.split(';'):
            key, value = option.split('=')
            nics[-1][key] = value

    resp = nova.servers.create(
        name=name,
        flavor=flavor_id,
        security_groups=security_groups.split(','),
        userdata=str(combine_files(file_contents(userdata))),
        image=image_id,
        nics=nics,
        availability_zone=availability_zone,
        key_name=key_name)

    server_id = resp.id
    ip_address = wait_for(lambda: server_address(nova, resp.id),
                          "Server never got an IP address.")
    print server_id
    print ip_address


@task
@verbose
def list_host_aggregates(availability_zone, hostname=[]):
    """Prints a pretty table of hosts in for each aggregate in AZ

       :param str availability_zone: The availability zone that the aggregates
         are in
       :param str hostname: Only display hostname in table. Use multiple times
         for more then one host
    """

    nova = client()

    # filters for aggregates in availability_zone
    aggregate_list = nova.aggregates.list()
    aggregates = []
    for aggregate in aggregate_list:
        if aggregate.availability_zone == availability_zone:
            aggregates.append(aggregate)

    hosts = []
    # loads hosts from aggregates if not specified
    if not hostname:
        for aggregate in aggregates:
            hosts.extend(aggregate.hosts)
    else:
        hosts = hostname

    # unique hosts
    hosts = list(set(hosts))
    hosts.sort()

    # builds table
    header = ["Aggregates"] + hosts
    table = PrettyTable(header)
    table.align["Aggregates"] = 'l'
    aggregates.sort()
    for aggregate in aggregates:
        row = [aggregate.name]
        for host in hosts:
            if host in aggregate.hosts:
                row.append("X")
            else:
                row.append("")
        table.add_row(row)

    print table
